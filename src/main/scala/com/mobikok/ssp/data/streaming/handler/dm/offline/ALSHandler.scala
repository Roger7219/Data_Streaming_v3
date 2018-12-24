package com.mobikok.ssp.data.streaming.handler.dm.offline

import java.text.SimpleDateFormat
import java.util.Date

import com.mobikok.message.client.MessageClient
import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.config.RDBConfig
import com.mobikok.ssp.data.streaming.exception.HandlerException
import com.mobikok.ssp.data.streaming.util.{CSTTime, Logger, MySqlJDBCClient}
import com.typesafe.config.Config
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{expr, _}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by Administrator on 2017/7/13.
  */
class ALSHandler extends Handler {

//  private val simpleDateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")

  var dwrTable: String = null
  var modelOutputDir:String = null

  val seqTimeFormat = CSTTime.formatter("yyyyMMdd_HHmmss_SSS")//new SimpleDateFormat("yyyyMMdd_HHmmss_SSS")

  override def init (moduleName: String, bigQueryClient: BigQueryClient, greenplumClient:GreenplumClient, rDBConfig:RDBConfig, kafkaClient: KafkaClient, messageClient: MessageClient, kylinClientV2: KylinClientV2, hbaseClient: HBaseClient, hiveContext: HiveContext, handlerConfig: Config): Unit = {

    super.init(moduleName, bigQueryClient, greenplumClient, rDBConfig, kafkaClient: KafkaClient, messageClient, kylinClientV2, hbaseClient, hiveContext, handlerConfig)
    dwrTable = handlerConfig.getString("dwr.table")
    modelOutputDir = handlerConfig.getString("model.output.dir")
  }

  override def handle (): Unit = {
    try {

      val dwr = hiveContext
        .read
        .table(dwrTable)
        .rdd

      val ratings = dwr
        .map{x=>
          Rating(x.getAs[Int]("userId"), x.getAs[Int]("offerId"), x.getAs[Int]("rating").toDouble)
        }

      LOG.warn(s"ALSHandler loaded ratings from table $dwrTable, take(10)", ratings.take(10))

      val users = ratings.map(_.user).distinct()
      val products = ratings.map(_.product).distinct()
      LOG.warn(s"ALSHandler ratings stat", "Got "+ratings.count()+" ratings from "+users.count+" users on "+products.count+" products.")


      //训练模型
      val rank = 12
      val lambda = 0.01
      val numIterations = 20
      val model = ALS.train(ratings, rank, numIterations, lambda)

      LOG.warn(s"ALSHandler model train completed", s"productFeatures.count: ${model.productFeatures.count()}, userFeatures.count: ${model.userFeatures.count()}")

      val f =s"$modelOutputDir/${seqTimeFormat.format(new Date)}.model"

      model.save(hiveContext.sparkContext, f)
      LOG.warn("ALSHandler model saved, output file", f)

      val savedModel:MatrixFactorizationModel = MatrixFactorizationModel.load(hiveContext.sparkContext, f);

      users
        .take(3)
        .foreach{x=>
          LOG.warn(s"ALSHandler recommendProducts by userId on savedModel $x", savedModel.recommendProducts(x,10))
        }

      users
        .take(3)
        .foreach{x=>
          LOG.warn(s"ALSHandler recommendProducts by userId on memoryModel $x", model.recommendProducts(x,10))
        }

//      val ui = dwr.map{x=> (x.getAs[Int]("userId"), x.getAs[String]("imei"))}.distinct()
//      ui
//      model
//        .recommendProductsForUsers(20)
//        .join(ui)
//        .map{x=>
//          x._2.foreach{y=>
////            com.mobikok.ssp.data.streaming.entity.Rating(y.user, y y.product, y.rating)
//
//          }
//        }
//      hbaseClient.putsNonTransaction("", )



      //val validationRmse  = computeRmse(model, ratings)
      //LOG.warn("Mean Squared Error ", validationRmse)

    } catch {
      case e: Exception => {
        throw new HandlerException(classOf[ALSHandler].getSimpleName + " Handle Fail：", e)
      }
    }

  }


  /** Compute RMSE (Root Mean Squared Error). */
  def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating]) = {
    val usersProducts = data.map { case Rating(user, product, rate) =>
      (user, product)
    }

    val predictions = model.predict(usersProducts).map { case Rating(user, product, rate) =>
      ((user, product), rate)
    }

    val ratesAndPreds = data.map { case Rating(user, product, rate) =>
      ((user, product), rate)
    }.join(predictions).sortByKey()

    math.sqrt(ratesAndPreds.map { case ((user, product), (r1, r2)) =>
      val err = (r1 - r2)
      err * err
    }.mean())
  }

}
