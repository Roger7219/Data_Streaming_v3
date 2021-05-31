#生产部署目录data-streaming v3.2:/apps/data-streaming/目录
#规范是定义三个配置文件：
app.conf：放module配置  
app.sh 放启动spark-submit命令  
supervisord.conf 放supervisor的配置,部署的时候放到生产的/etc/supervisord.conf里面  
打包的时候,不用把依赖包打进去data-streaming.jar内,打项目本身代码就行,7M左右,通过app.sh内的jars从公共的包目录里引入依赖包

#clickhouse登陆:
clickhouse-client -m -h node110 --port 9000  --password 9036011d8fa79028f99488c2de6fdfb09d66beb5e4b8ca5178ee9ff9179ed6a8

clickhouse-client -m -h node111 --port 9000  --password 9036011d8fa79028f99488c2de6fdfb09d66beb5e4b8ca5178ee9ff9179ed6a8

clickhouse-client -m -h node16 --port 9000  --password 9036011d8fa79028f99488c2de6fdfb09d66beb5e4b8ca5178ee9ff9179ed6a8

clickhouse-client -m -h node15 --port 8000  --password 9036011d8fa79028f99488c2de6fdfb09d66beb5e4b8ca5178ee9ff9179ed6a8

# 监控报表中:null或者-1 说明该批次刚启动，还没完成，等完成了有值了，值就是该批次的总耗时

# 常见问题
1.渠道bd修改以及转接,需要重新同步数据,刷新BI报表  
涉及到hive中的表:advertiser,publisher,employee  具体以业务为准  
①确认修改数据  
②分别清空涉及到的hive表数据 TRUNCATE TABLE default.advertiser;  
③等到清空表数据重新拉取到数据以后进行BI报表刷新  
④登录远程桌面,刷新BI报表,按业务要求执行CrontabClickhouseRefreshUtil.refreshDay 或 CrontabClickhouseRefreshUtil.refreshHour方法  

2.Ssp系统明细数据异常,和BI报表中明细数据不一致,需要重新同步bigquery数据  
①确认异常数据时间  
②登录远程桌面,刷新bigquery数据,按业务要求执行CrontabClickhouseRefreshUtil.refreshHour_ssp_overall_postback_dwi方法  