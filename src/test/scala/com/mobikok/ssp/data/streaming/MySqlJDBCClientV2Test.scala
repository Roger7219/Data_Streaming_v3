package com.mobikok.ssp.data.streaming

import com.mobikok.ssp.data.streaming.util.MySqlJDBCClient

/**
  * Created by Administrator on 2017/8/7.
  */
object MySqlJDBCClientV2Test {

  def main (args: Array[String]): Unit = {

    val rdbUser = "root"
//    val rdbPassword = "@dfei$@DCcsYG"//
    val rdbPassword = "root_root"
    //
    val rdbUrl = "jdbc:mysql://node17:3306/test?rewriteBatchedStatements=true&autoReconnect=true&useUnicode=true&characterEncoding=utf8"
//    val rdbUrl = "jdbc:mysql://104.250.131.130:8904/kok_ssp?rewriteBatchedStatements=true&autoReconnect=true&useUnicode=true&characterEncoding=utf8"
//    val rdbUrl = "jdbc:mysql://192.168.1.244:8904/kok_ssp?rewriteBatchedStatements=true&autoReconnect=true&useUnicode=true&characterEncoding=utf8"
    val rdbProp = new java.util.Properties {
      {
        setProperty("user", rdbUser)
        setProperty("password", rdbPassword)
        setProperty("driver", "com.mysql.jdbc.Driver") //must set!
      }
    }

    val mySqlJDBCClient = new MySqlJDBCClient(
      "MySqlJDBCClientV2Test", rdbUrl, rdbUser, rdbPassword
    )
    val sqls =
      s"""
         |INSERT INTO testx(appId,
         |        imsi,
         |        imei,
         |        countryId,
         |        carrierId,
         |        model,
         |        version,
         |        sdkVersion,
         |        installType,
         |        leftSize,
         |        androidId,
         |        ipAddr,
         |        screen,
         |        userAgent,
         |        sv,
         |        createTime
         | )
         |  VALUES(?,?,?,? ,?,?,?,? ,?,?,?,? ,?,?,?,?)
       """.stripMargin

    val map = Map(
      1   -> 2553,
      2   -> "639021018274367",
      3   -> "358808085733529",
      4   -> 106,
      5   -> 2,
      6   -> "Infinix X559",
      7   -> "7.0",
      8   -> 0,
      9   -> 0,
      10  -> "4296",
      11  -> "c9980f7b360c8b3a",
      12  -> "197.231.181.44",
      13  -> "720x1200",
      14  -> "Mozilla/5.0 (Linux; Android 7.0; Infinix X559 Build/NRD90M; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/58.0.3029.83 Mobile Safari/537.36 ",
      15  -> "v3.0.5",
      16  -> "2017-12-14 18:55:03"

    )

    mySqlJDBCClient.executeBatch(sqls,map)

//    mySqlJDBCClient.execute(
//      s"""
//         |create table testx(
//         |  appId int(11) NOT NULL DEFAULT '0',
//         |  imsi varchar(256) NOT NULL,
//         |  imei varchar(256) NOT NULL,
//         |  countryId int(11) NOT NULL DEFAULT '0',
//         |  carrierId int(11) NOT NULL DEFAULT '0',
//         |  model varchar(256) NOT NULL,
//         |  version varchar(256) NOT NULL,
//         |  sdkVersion int(11) NOT NULL DEFAULT '0',
//         |  installType int(11) NOT NULL DEFAULT '0',
//         |  leftSize varchar(256) NOT NULL,
//         |  androidId varchar(256) NOT NULL,
//         |  ipAddr varchar(256) NOT NULL,
//         |  screen varchar(256) NOT NULL,
//         |  userAgent varchar(256) NOT NULL,
//         |  sv varchar(256) NOT NULL,
//         |  createTime varchar(256) NOT NULL
//         |
//         |) ENGINE=MariaDB DEFAULT CHARSET=utf8
//         |
//       """.stripMargin
//    )


//    mySqlJDBCClient.execute(
//      s"""
//         |    insert into module_running_status(
//         |    module_name,
//         |    batch_using_time,
//         |    update_time
//         |  )
//         |  values(
//         |    "fee",
//         |    577.5158,
//         |    now()
//         |  )
//         |  on duplicate key update batch_using_time = values(batch_using_time)
//       """.stripMargin)
////    mySqlJDBCClient.executeQuery("show tables");
  }
}
