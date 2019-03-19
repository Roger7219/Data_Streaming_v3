package com.mobikok.ssp.data.streaming.module.support

import java.util
import java.util.Date

import com.mobikok.ssp.data.streaming.client.MixTransactionManager
import com.mobikok.ssp.data.streaming.exception.AppException
import com.mobikok.ssp.data.streaming.util.{Logger, OM}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConversions._

/**
  * Created by Administrator on 2017/10/16.
  */
class ShareDwrTableModulesBatchController (dwrShareTable: String, mixTransactionManager: MixTransactionManager, hiveContext: HiveContext, shufflePartitions: Int) {

  val LOG: Logger = new Logger(getClass.getName, new Date().getTime)

  //moduleName, unionReadied, isMaster
  @volatile private var moduleNamesMappingCurrBatchModuleUnionReadied: java.util.HashMap[String, Boolean] = new util.HashMap[String, Boolean]()

  //配置文件里的设置master=true
  //moduleName, isMaster
  @volatile private var moduleIsMasterOfSettingFilePlainVlaueMap: java.util.HashMap[String, Boolean] = new util.HashMap[String, Boolean]()

  @volatile private var cacheGroupByDwr: DataFrame = null
  @volatile private var lock = new Object

  @volatile private var currBatchUnionedAll = false

  def isMultipleModulesOperateSameDwrTable (): Boolean = {
    moduleNamesMappingCurrBatchModuleUnionReadied.size() > 1
  }

  def isMaster(moduleName: String): Boolean = {
    if(isMultipleModulesOperateSameDwrTable) {
      moduleIsMasterOfSettingFilePlainVlaueMap.get(moduleName)
    }else {
      true
    }
  }

  def assertJustOnlyOneMasterModule () {
    var i = 0
    moduleIsMasterOfSettingFilePlainVlaueMap.foreach{x=>
      if(isMultipleModulesOperateSameDwrTable) {
        if(x._2) {
          i = i + 1
        }
        //如果只有一个模块，那它属于master
      }else{
        i = i +1
      }
    }

    if(i > 1) {
      throw new AppException(s"Too many modules settings for dwr table '$dwrShareTable', Make sure that only one module is master, plain settings(is master):\n" +OM.toJOSN(moduleIsMasterOfSettingFilePlainVlaueMap))
    }
    else if(i == 0){
      throw new AppException(s"No master module specified for dwr table '$dwrShareTable', plain settings(is master):\n" + OM.toJOSN(moduleIsMasterOfSettingFilePlainVlaueMap) )
    }
  }
  def isContainsModule (moduleName: String): Boolean = {
    moduleNamesMappingCurrBatchModuleUnionReadied.containsKey(moduleName)
  }

  def waitUnionAll (appendDwr: DataFrame, isMasterModule: Boolean, moduleName:String): Unit = {

    synchronizedCall(new Callback {
      override def onCallback (): Unit = {

        LOG.warn(s"$moduleName - ShareTableMoudlesBatchController union starting")
        //效验状态
        if(moduleNamesMappingCurrBatchModuleUnionReadied.get(moduleName)) {
          throw new RuntimeException(s"The current batch module '$moduleName' is ready. You cannot store DWR repeatedly")
        }

        var old = cacheGroupByDwr

        if (cacheGroupByDwr == null) {
          cacheGroupByDwr = appendDwr
        } else {
          cacheGroupByDwr = cacheGroupByDwr.union(appendDwr)
        }

        moduleNamesMappingCurrBatchModuleUnionReadied.put(moduleName, true)

        LOG.warn(s"$moduleName - curr batch module union readied",
          s"""
             |tid: ${mixTransactionManager.getCurrentTransactionParentId()}
             |union readied: ${OM.toJOSN(moduleNamesMappingCurrBatchModuleUnionReadied)}
          """.stripMargin)

//        LOG.warn(s"$moduleName - curr batch module union readied",
//          s"""
//             |tid: ${mixTransactionManager.getCurrentTransactionParentId()}
//             |union readied: ${OM.toJOSN(moduleNamesMappingCurrBatchModuleUnionReadied)}
//             |old count: ${if(old == null) "null" else old.count()}
//             |append count: ${if(appendDwr == null) "null" else appendDwr.count()}
//             |new count: ${if(cacheGroupByDwr == null) "null" else cacheGroupByDwr.count()}
//          """.stripMargin)

        LOG.warn(s"$moduleName - ShareTableMoudlesBatchController union done")
      }
    }, lock)

    //Wait for all readied
    var b = true
    while(b) {
      var allReadied = true
      synchronizedCall(new Callback {
        override def onCallback (): Unit = {
          moduleNamesMappingCurrBatchModuleUnionReadied.entrySet().foreach{x=>
            if(!x.getValue && allReadied) {
              allReadied = false
            }
          }
        }
      },lock)

      if(allReadied) {
        b = false
      }else {
        Thread.sleep(100)
      }
    }

    synchronizedCall(new Callback {
      override def onCallback (): Unit = {
        if (isMasterModule && !currBatchUnionedAll/*!currBatchUnionedAll*/) {
          LOG.warn("ShareTableMoudlesBatchController refresh final df starting")
          //   ???       cacheGroupByDwr = hiveContext.createDataFrame(cacheGroupByDwr.collectAsList(), cacheGroupByDwr.schema).repartition(shufflePartitions)
          cacheGroupByDwr.persist(StorageLevel.MEMORY_ONLY_SER)
          cacheGroupByDwr.count()
          currBatchUnionedAll = true
          LOG.warn("ShareTableMoudlesBatchController refresh final df done")
        }
      }
    }, lock)

  }

  def get (): DataFrame = {
    var res = null.asInstanceOf[DataFrame]
    synchronizedCall(new Callback {
      override def onCallback (): Unit = {
        while(!currBatchUnionedAll) {
          Thread.sleep(1000*1)
        }
        res = cacheGroupByDwr
      }
    }, lock)
    res
  }

  def set (newDF: => DataFrame): Unit = {
    synchronizedCall(new Callback {
      override def onCallback (): Unit = {
        cacheGroupByDwr = newDF
      }
    }, lock)
  }

  def completeBatch (isMasterModule: Boolean): Unit = {
    synchronizedCall(new Callback {
      override def onCallback (): Unit = {
        if (isMasterModule) {
          currBatchUnionedAll = false
          if(cacheGroupByDwr != null) cacheGroupByDwr.unpersist()
          cacheGroupByDwr = null

          moduleNamesMappingCurrBatchModuleUnionReadied.entrySet().foreach{x=>
            moduleNamesMappingCurrBatchModuleUnionReadied.put(x.getKey, false)
          }
        }
      }
    }, lock);
  }

  def addMoudleName (moudleName: String, isMaster:Boolean): Unit = {
    synchronizedCall(new Callback {
      override def onCallback (): Unit = {
        moduleNamesMappingCurrBatchModuleUnionReadied.put(moudleName, false)
        moduleIsMasterOfSettingFilePlainVlaueMap.put(moudleName, isMaster)
        mixTransactionManager.addModuleName(moudleName)
      }
    }, lock)

  }

  def getMixTransactionManager (): MixTransactionManager = {
    mixTransactionManager
  }

  def synchronizedCall (callback: Callback, lock: Object): Unit = {
    lock.synchronized {
      callback.onCallback()
    }
  }


  trait Callback {
    def onCallback ()
  }

}


