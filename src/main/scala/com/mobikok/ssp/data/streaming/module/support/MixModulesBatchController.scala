package com.mobikok.ssp.data.streaming.module.support

import java.util
import java.util.Date

import com.mobikok.ssp.data.streaming.client.MixTransactionManager
import com.mobikok.ssp.data.streaming.exception.{AppException, ModuleException}
import com.mobikok.ssp.data.streaming.util.{Logger, OM}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel
import com.typesafe.config.Config

import scala.collection.JavaConversions._
import org.apache.spark.sql.functions._

/**
  * Created by Administrator on 2017/10/16.
  */
class MixModulesBatchController(config:Config, runnableModuleNames: Array[String], dwrShareTable: String, mixTransactionManager: MixTransactionManager, hiveContext: HiveContext, shufflePartitions: Int) {

  def isRunnable(moduleName: String): Boolean = runnableModuleNames.contains(moduleName)
  def isInitable(moduleName: String): Boolean = {
      var result = false

      if(runnableModuleNames.contains(moduleName)) {
        result = true
      } else {
        val sms = mixTransactionManager.prevRunningSameTransactionGroupModules(moduleName)
        result = sms.contains(moduleName)
      }
      result
  }

  val LOG: Logger = new Logger(getClass.getName, new Date().getTime)

  //moduleName, unionReadied, isMaster
  @volatile private var moduleNamesMappingCurrBatchModuleUnionReadied: java.util.HashMap[String, Boolean] = new util.HashMap[String, Boolean]()

  //all mix module
  @volatile private var allMixModuleNames: util.HashSet[String] = new util.HashSet[String]()

  //配置文件里的设置master=true
  //moduleName, isMaster
  @volatile private var moduleIsMasterOfSettingFilePlainVlaueMap: java.util.HashMap[String, Boolean] = new util.HashMap[String, Boolean]()

  @volatile private var cacheGroupByDwr: DataFrame = null

  @volatile private var lock = new Object

  @volatile private var currBatchUnionAllAndPersisted = false


  var dwrGroupByUnionAggExprsAndAlias:List[Column] = null
  var dwrGroupByDimensionFieldsAlias:List[Column] = null

  def isMultipleModulesOperateSameDwrTable (): Boolean = {
    moduleNamesMappingCurrBatchModuleUnionReadied.size() > 1
  }

  def isMaster(moduleName: String): Boolean = {
    if(isMultipleModulesOperateSameDwrTable) {
      moduleIsMasterOfSettingFilePlainVlaueMap.get(moduleName)
    }else {
      //如果只有一个模块，那它属于master
      true
    }
  }

  def assertJustOnlyOneMasterModule () {
    var i = 0
    if(moduleIsMasterOfSettingFilePlainVlaueMap.size() > 0) {
      moduleIsMasterOfSettingFilePlainVlaueMap.foreach { x =>
        if (isMultipleModulesOperateSameDwrTable) {
          if (x._2) {
            i = i + 1
          }
          //如果只有一个模块，那它属于master
        } else {
          i = i + 1
        }
      }

      if (i > 1) {
        throw new AppException(s"Too many modules settings for dwr table '$dwrShareTable', Make sure that only one module is master, plain settings(is master):\n" + OM.toJOSN(moduleIsMasterOfSettingFilePlainVlaueMap))
      }
      else if (i == 0) {
        throw new AppException(s"No master module specified for dwr table '$dwrShareTable', plain settings(is master):\n" + OM.toJOSN(moduleIsMasterOfSettingFilePlainVlaueMap))
      }
    }
  }
  def isContainsModule (moduleName: String): Boolean = {
    allMixModuleNames.contains(moduleName)
  }

  def runnableModules(): Array[String] ={
    moduleNamesMappingCurrBatchModuleUnionReadied.map(_._1).toArray
  }

  def modules(): Array[String] = {
    allMixModuleNames.toArray[String](new Array[String](0))
  }

  def waitUnionAll (appendDwr: DataFrame, isMasterModule: Boolean, moduleName:String): Unit = {

    synchronizedCall(new Callback {
      override def onCallback (): Unit = {

        LOG.warn(s"$moduleName - MixModulesBatchController union start")
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
          s"""tid: ${mixTransactionManager.getCurrentTransactionParentId()}
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

        LOG.warn(s"$moduleName - MixModulesBatchController union done")
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

    LOG.warn("All modules union done")

    if(isMasterModule && !currBatchUnionAllAndPersisted) {
      LOG.warn("MixModulesBatchController persist final df start")

      cacheGroupByDwr = cacheGroupByDwr
        .groupBy(col("l_time") :: col("b_date") :: col("b_time"):: col("b_version") :: dwrGroupByDimensionFieldsAlias: _*)
        .agg(dwrGroupByUnionAggExprsAndAlias.head, dwrGroupByUnionAggExprsAndAlias.tail: _*)

      cacheGroupByDwr.persist(StorageLevel.MEMORY_ONLY_SER)

      cacheGroupByDwr.count()//触发persist
      currBatchUnionAllAndPersisted = true
      LOG.warn("MixModulesBatchController persist final df done")

    }

  }

  def get (): DataFrame = {
    var res = null.asInstanceOf[DataFrame]
    synchronizedCall(new Callback {
      override def onCallback (): Unit = {
        //等待persist完成
        while(!currBatchUnionAllAndPersisted) {
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
          currBatchUnionAllAndPersisted = false
          if(cacheGroupByDwr != null) cacheGroupByDwr.unpersist()
          cacheGroupByDwr = null

          moduleNamesMappingCurrBatchModuleUnionReadied.entrySet().foreach{x=>
            moduleNamesMappingCurrBatchModuleUnionReadied.put(x.getKey, false)
          }
        }
      }
    }, lock);
  }

  def addMoudle(moduleName: String, isMasterOfConfigSpecify:Boolean): Unit = {
    synchronizedCall(new Callback {
      override def onCallback (): Unit = {
        LOG.warn("Add Moudle", "moudleName", moduleName, "isMasterOfConfigSpecify", isMasterOfConfigSpecify, "runnableModuleNames", runnableModuleNames)
        if(runnableModuleNames.contains(moduleName)) {
          moduleNamesMappingCurrBatchModuleUnionReadied.put(moduleName, false)
          moduleIsMasterOfSettingFilePlainVlaueMap.put(moduleName, isMasterOfConfigSpecify)
          mixTransactionManager.addModuleName(moduleName)
        }
        allMixModuleNames.add(moduleName)

        //初始化以及检验聚合操作的配置
        if(dwrGroupByDimensionFieldsAlias == null) {
          try {
            dwrGroupByDimensionFieldsAlias = config.getConfigList(s"modules.$moduleName.dwr.groupby.fields").map {
              x => expr(x.getString("as"))
            }.toList

            dwrGroupByUnionAggExprsAndAlias = config.getConfigList(s"modules.$moduleName.dwr.groupby.aggs").map {
              x => expr(x.getString("union")).as(x.getString("as"))
            }.toList

          }catch {case e:Exception=>}
        }else {
          //验证同一组module 配置是否一致
          var _dwrGroupByDimensionFieldsAlias = config.getConfigList(s"modules.$moduleName.dwr.groupby.fields").map {
              x => expr(x.getString("as"))
          }.toList

          var _dwrGroupByUnionAggExprsAndAlias = config.getConfigList(s"modules.$moduleName.dwr.groupby.aggs").map {
            x => expr(x.getString("union")).as(x.getString("as"))
          }.toList


          if(!_dwrGroupByDimensionFieldsAlias.toString().equals(dwrGroupByDimensionFieldsAlias.toString())) {
            throw new ModuleException(s"Mix module dwr dimension fields alias is not the same ! \n$moduleName config: ${_dwrGroupByDimensionFieldsAlias}, \nfirst module config: $dwrGroupByDimensionFieldsAlias")
          }

          if(!_dwrGroupByUnionAggExprsAndAlias.toString().equals(dwrGroupByUnionAggExprsAndAlias.toString())) {
            throw new ModuleException(s"Mix module dwr union-agg is not the same ! \n$moduleName config: ${_dwrGroupByUnionAggExprsAndAlias}, \nfirst module config: $dwrGroupByUnionAggExprsAndAlias")
          }

        }

      }
    }, lock)
  }

  //  def addMoudle(m: Module): Unit = {
//    synchronizedCall(new Callback {
//      override def onCallback (): Unit = {
//        if(m.isRunnable()) {
//          val moudleName = m.getName()
//          moduleNamesMappingCurrBatchModuleUnionReadied.put(moudleName, false)
//          moduleIsMasterOfSettingFilePlainVlaueMap.put(moudleName, m.isMaster())
//          mixTransactionManager.addMoudleName(moudleName)
//        }
//      }
//    }, lock)
//
//    //效验
//    assertJustOnlyOneMasterModule()
//  }

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



object xxtest{

  def main(args: Array[String]): Unit = {
    println(List(col("x"), col("ee")).equals(List(col("x"), col("ee"))))
  }

}
