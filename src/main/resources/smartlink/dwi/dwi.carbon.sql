
spark-shell
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.CarbonSession._

var carbon = SparkSession.builder().enableHiveSupport().config(sc.getConf).getOrCreateCarbonSession
carbon.sql("""
create table smartlink_click_dwi(
        repeats        INT   ,
        rowkey         STRING,
        id             INT   ,
        publisherId    INT   , -- + AM
        subId          INT   , -- + APP
        offerId        INT   ,
        campaignId     INT   , -- + campaign > 上级ADVER
        countryId      INT   ,
        carrierId      INT   ,
        deviceType     INT   ,
        userAgent      STRING,
        ipAddr         STRING,
        clickId        STRING,
        price          DOUBLE,
        reportTime     STRING,
        createTime     STRING,
        clickTime      STRING,
        showTime       STRING,
        requestType    STRING,
        priceMethod    INT   ,  -- 1 cpc(click), 2 cpm(show), 3 cpa(转化数=计费数?)
        bidPrice       DOUBLE,
        adType         INT   ,
        isSend         INT   ,
        reportPrice    DOUBLE,
        sendPrice      DOUBLE,
        s1             STRING,
        s2             STRING,
        gaid           STRING,
        androidId      STRING,
        idfa           STRING,
        postBack       STRING,
        sendStatus     INT   ,
        sendTime       STRING,
        sv             STRING,
        imei           STRING,
        imsi           STRING,
        imageId        INT   ,
        affSub         INT   ,
        s3             STRING,
        s4             STRING,
        s5             STRING,
        packageName    STRING,
        domain         STRING,
        respStatus     INT   , --  未下发原因
        winPrice       DOUBLE,
        winTime        STRING,
        appprice       DOUBLE,
        test           INT   ,
        ruleid         INT   ,
        smartid        INT   ,
        pricepercent   INT   ,
        apppercent     INT   ,
        salepercent    INT   ,
        appsalepercent INT   ,
        reportip       STRING,
        eventname      STRING,
        eventvalue     INT   ,
        refer          STRING,
        status         INT   ,
        city           STRING,
        region         STRING,
        uid            STRING,
        times          INT   ,
        time           INT   ,
        isnew          INT   ,
        pbresp         STRING,
        recommender    INT   ,
        ratertype      INT   ,
        raterid        STRING
)
PARTITIONED BY (repeated STRING, l_time STRING, b_date STRING, b_time STRING)
STORED AS carbondata
""")

carbon.sql("""
create table smartlink_send_dwi(
        repeats        INT   ,
        rowkey         STRING,
        id             INT   ,
        publisherId    INT   , -- + AM
        subId          INT   , -- + APP
        offerId        INT   ,
        campaignId     INT   , -- + campaign > 上级ADVER
        countryId      INT   ,
        carrierId      INT   ,
        deviceType     INT   ,
        userAgent      STRING,
        ipAddr         STRING,
        clickId        STRING,
        price          DOUBLE,
        reportTime     STRING,
        createTime     STRING,
        clickTime      STRING,
        showTime       STRING,
        requestType    STRING,
        priceMethod    INT   ,  -- 1 cpc(click), 2 cpm(show), 3 cpa(转化数=计费数?)
        bidPrice       DOUBLE,
        adType         INT   ,
        isSend         INT   ,
        reportPrice    DOUBLE,
        sendPrice      DOUBLE,
        s1             STRING,
        s2             STRING,
        gaid           STRING,
        androidId      STRING,
        idfa           STRING,
        postBack       STRING,
        sendStatus     INT   ,
        sendTime       STRING,
        sv             STRING,
        imei           STRING,
        imsi           STRING,
        imageId        INT   ,
        affSub         INT   ,
        s3             STRING,
        s4             STRING,
        s5             STRING,
        packageName    STRING,
        domain         STRING,
        respStatus     INT   , --  未下发原因
        winPrice       DOUBLE,
        winTime        STRING,
        appprice       DOUBLE,
        test           INT   ,
        ruleid         INT   ,
        smartid        INT   ,
        pricepercent   INT   ,
        apppercent     INT   ,
        salepercent    INT   ,
        appsalepercent INT   ,
        reportip       STRING,
        eventname      STRING,
        eventvalue     INT   ,
        refer          STRING,
        status         INT   ,
        city           STRING,
        region         STRING,
        uid            STRING,
        times          INT   ,
        time           INT   ,
        isnew          INT   ,
        pbresp         STRING,
        recommender    INT   ,
        ratertype      INT   ,
        raterid        STRING
)
PARTITIONED BY (repeated STRING, l_time STRING, b_date STRING, b_time STRING)
STORED AS carbondata
""")


create table smartlink_fill_dwi like smartlink_click_dwi;
