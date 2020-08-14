package com.mobikok.smartlink;

import com.mobikok.ssp.data.streaming.util.MySqlJDBCClient;
import com.mobikok.ssp.data.streaming.util.OM;
import io.codis.jodis.JedisResourcePool;
import io.codis.jodis.RoundRobinJedisPool;
import redis.clients.jedis.Jedis;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class SmartlinkConfigV2 {

	static String ZK_PROXY_DIR = "/zk/codis/db_kok_adv/proxy";
//	static String HOST_PORT = "104.250.141.178:2181";
	static String HOST_PORT = "192.168.111.2:2181";
	private static JedisResourcePool jedisPool = RoundRobinJedisPool.create().curatorClient(HOST_PORT, 30000).zkProxyDir(ZK_PROXY_DIR).build();

	// node14:  104.250.136.138
	// node103: 104.250.132.242
//	private static String RECOMMEDND_IP = "s1.node103.iifab.com"; //"104.250.132.242";
//	private static String MINE_IP = "s1.node104.iifab.com"; //"104.250.141.66";

	private static String RECOMMEDND_IP = "s1.node103.iifab.com"; //"104.250.132.242";
	private static String MINE_IP = "noadx.net"; //"104.250.141.66";


	//[{"id":1,"url":"","weight":50,"countryIds":"","type":0},{"id":1030001,"url":"http://104.250.136.138:3333/api/smartlink?s=2708&at=4&rt=api&s1={s1}&s2={s2}&s3={s3}&s4={s4}&s5={s5}","weight":50,"countryIds":"","type":0}]
	private static MySqlJDBCClient mySqlJDBCClient = new MySqlJDBCClient(
			"SmartlinkConfigV2",
			"jdbc:mysql://192.168.111.12:8904/kok_ssp?rewriteBatchedStatements=true&autoReconnect=true&useUnicode=true&characterEncoding=utf8",
			"root",
			"@dfei$@DCcsYG"
			);
    	
        	
	public static void main(String[] args) {

		System.out.println("start.................");

		Integer _ssp_sagacityWeight;
		Integer _recom_sagacityWeight;
		Integer _mine_sagacityWeight;

		try {
			_ssp_sagacityWeight = Integer.valueOf(args[0]);
			_recom_sagacityWeight = Integer.valueOf(args[1]);
			_mine_sagacityWeight = Integer.valueOf(args[2]);

		}catch (Exception e){
			_ssp_sagacityWeight = 10;
			_recom_sagacityWeight = 90;
			_mine_sagacityWeight = 0;
		}

		final Integer ssp_sagacityWeight = _ssp_sagacityWeight;
		final Integer recom_sagacityWeight = _recom_sagacityWeight;
		final Integer mine_sagacityWeight = _mine_sagacityWeight;


		mySqlJDBCClient.execute("delete from OTHER_SMART_LINK where name like '推荐系统测试App_%' ");
		mySqlJDBCClient.execute("update APP set SmartConfig = '' where SmartConfig like '%:3333/api/smartlink%' ");
		// 待删
		mySqlJDBCClient.execute("update APP set SmartConfig = '' where SmartConfig like '%:8888/api/smartlink%' ");
        //正式
		mySqlJDBCClient.execute("update APP set SmartConfig = '' where SmartConfig like '%noadx.net/redirect%' ");
//	
		final List<String> linkSqls = new ArrayList<String>();
		final List<String> appSmartConfigSqls = new ArrayList<String>();
	
		final Integer[] appIds = null;

		final Jedis jedis = jedisPool.getResource();

		final int[] linkStartId = {1000000};
		mySqlJDBCClient.executeQuery(
				"SELECT id, publisherId FROM APP where mode = 2 ", 
				new MySqlJDBCClient.Callback<Object>(){

					public Object onCallback(final ResultSet rs) {
						try {
							List<Integer> is = appIds == null ? null : Arrays.asList(appIds);
							while(rs.next()) {
								if(is == null || is.contains(rs.getInt("id"))) {

									// recommend
									linkSqls.add("insert into OTHER_SMART_LINK ("
											  + "Id, "
											  + "Name, "
											  + "PublisherId, "
											  + "Link, "
											  + "CreateTime, "
											  + "`Desc`, "
											  + "Uniqueid"
											+ ")values("
											+ linkStartId[0] + ", "
											+ "'推荐系统测试App_recommend_"+rs.getInt("id") +"', "
											+ rs.getInt("publisherId")+", "
											+ "'http://"+ RECOMMEDND_IP +":3333/api/smartlink?s="+rs.getString("id")+"&at=4&rt=api&s1={s1}&s2={s2}&s3={s3}&s4={s4}&s5={s5}', "
											+ "now(), "
											+ "'-', "
											+ "'" + UUID.randomUUID().toString() +"'"
											+ ")");

									// mine
									linkSqls.add("insert into OTHER_SMART_LINK ("
											+ "Id, "
											+ "Name, "
											+ "PublisherId, "
											+ "Link, "
											+ "CreateTime, "
											+ "`Desc`, "
											+ "Uniqueid"
											+ ")values("
											+ (linkStartId[0] + 1) + ", "
											+ "'推荐系统测试App_mine_"+rs.getInt("id") +"', "
											+ rs.getInt("publisherId")+", "
											+ "'http://"+ MINE_IP +":8888/redirect?s="+rs.getString("id")+"&at=4&rt=api&s1={s1}&s2={s2}&s3={s3}&s4={s4}&s5={s5}', "
											+ "now(), "
											+ "'-', "
											+ "'" + UUID.randomUUID().toString() +"'"
											+ ")");

									final String recom_link = "http://"+ RECOMMEDND_IP +":3333/api/smartlink?s="+rs.getString("id")+"&at=4&rt=api&s1={s1}&s2={s2}&s3={s3}&s4={s4}&s5={s5}";
									final String mine_link = "http://"+MINE_IP+":8888/redirect?s="+rs.getString("id")+"&at=4&rt=api&s1={s1}&s2={s2}&s3={s3}&s4={s4}&s5={s5}";

									String json = "";
									json = OM.toJOSN(new ArrayList<HashMap<String, Object>>(2){{

										// ssp
										add(new HashMap<String, Object>(5){{
											put("id", 1);
											put("url", "");
											put("weight", ssp_sagacityWeight);
											put("countryIds", "");
											put("type", 0);
										}});

										// recommend
										add(new HashMap<String, Object>(5){{
											put("id", linkStartId[0]);
											put("url", recom_link);
											put("weight", recom_sagacityWeight);
											put("countryIds", "");
											put("type", 0);
										}});

										// mine
										add(new HashMap<String, Object>(5){{
											put("id", (linkStartId[0] + 1));
											put("url", mine_link);
											put("weight", mine_sagacityWeight);
											put("countryIds", "");
											put("type", 0);
										}});
									}}, false);

									RedisTool.set("smart_flow:" + linkStartId[0], recom_link);
									RedisTool.set("smart_flow:" + (linkStartId[0] + 1), mine_link);

									appSmartConfigSqls.add("update APP set "
											+ "SmartConfig = '" + json + "' "
											+ "where id = " + rs.getInt("id") + " and ("
													  + "smartConfig is null "
													  + "or smartConfig = '' "
													+ " )" );

									linkStartId[0] = linkStartId[0] + 2;
								}
							}
						} catch (SQLException e) {
							e.printStackTrace();
						}
						return null;
					}
					
				});
		
		System.out.println("linkSqls size:" + linkSqls.size());
		System.out.println("linkSqls:  " + (linkSqls) );
		
		System.out.println("appSmartConfigSqls size:" + appSmartConfigSqls.size());
		System.out.println("appSmartConfigSqls: " + (appSmartConfigSqls));
		
		mySqlJDBCClient.executeBatch(linkSqls.toArray(new String[0]));
		 
		mySqlJDBCClient.executeBatch(appSmartConfigSqls.toArray(new String[0]));
		if(jedis != null) {
			jedis.close();
		}
		System.out.println("done.................");
		
		
	}
}
