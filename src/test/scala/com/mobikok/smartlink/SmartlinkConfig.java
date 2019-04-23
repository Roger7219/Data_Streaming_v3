package com.mobikok.smartlink;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import com.mobikok.ssp.data.streaming.util.MySqlJDBCClientV2;
import com.mobikok.ssp.data.streaming.util.OM;
import io.codis.jodis.JedisResourcePool;
import io.codis.jodis.RoundRobinJedisPool;
import redis.clients.jedis.Jedis;

public class SmartlinkConfig {

	static String ZK_PROXY_DIR = "/zk/codis/db_kok_adv/proxy";
	static String HOST_PORT = "104.250.141.178:2181";
	private static JedisResourcePool jedisPool = RoundRobinJedisPool.create().curatorClient(HOST_PORT, 30000).zkProxyDir(ZK_PROXY_DIR).build();

	// node14:  104.250.136.138
	// node103: 104.250.132.242
	private static String IP = "104.250.132.242";


	//[{"id":1,"url":"","weight":50,"countryIds":"","type":0},{"id":1030001,"url":"http://104.250.136.138:3333/api/smartlink?s=2708&at=4&rt=api&s1={s1}&s2={s2}&s3={s3}&s4={s4}&s5={s5}","weight":50,"countryIds":"","type":0}]
	private static MySqlJDBCClientV2 mySqlJDBCClient = new MySqlJDBCClientV2("",
			"jdbc:mysql://104.250.149.202:4000/kok_ssp?rewriteBatchedStatements=true&autoReconnect=true&useUnicode=true&characterEncoding=utf8",
			"root",
			"@dfei$@DCcsYG"
			);
    	
        	
	public static void main(String[] args) {

		System.out.println("start.................");

		Integer _sagacityWeight = 50;
		try {
			_sagacityWeight = Integer.valueOf(args[0]);
		}catch (Exception e){}
		final Integer sagacityWeight = _sagacityWeight;

		mySqlJDBCClient.execute("delete from OTHER_SMART_LINK where name like '推荐系统测试App_%' ");
		mySqlJDBCClient.execute("update APP set SmartConfig = '' where SmartConfig like '%:3333/api/smartlink%' ");
//	
		final List<String> linkSqls = new ArrayList<String>();
		final List<String> appSmartConfigSqls = new ArrayList<String>();
	
		final Integer[] appIds = null;
//				{
//			557623,2707,917597,497624,557624,1967593,497623,2708,1967594,
//			917595, 917594, 17670, 1277610, 407605, 407604, 1007612
//		};

		final Jedis jedis = jedisPool.getResource();

		final int[] linkStartId = {1000000};
		mySqlJDBCClient.executeQuery(
				"SELECT id, publisherId FROM APP where mode = 2 ", 
				new MySqlJDBCClientV2.Callback<Object>(){

					public Object onCallback(final ResultSet rs) {
						try {
							List<Integer> is = appIds == null ? null : Arrays.asList(appIds);
							while(rs.next()) {
								if(is == null || is.contains(rs.getInt("id"))) {
								
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
											+ "'推荐系统测试App_"+rs.getInt("id") +"', "
											+ rs.getInt("publisherId")+", "
											+ "'http://"+IP+":3333/api/smartlink?s="+rs.getString("id")+"&at=4&rt=api&s1={s1}&s2={s2}&s3={s3}&s4={s4}&s5={s5}', "
											+ "now(), "
											+ "'-', "
											+ "'" + UUID.randomUUID().toString() +"'"
											+ ")");
									
									
									// [{"id":1,"url":"","weight":70,"countryIds":"","type":0},{"id":28,"url":"https://c.navhi.com/ck/sl/Fpg1VJHD?tfc_id=199&sc={subid}&pub_click_id={clickid}","weight":30,"countryIds":"","type":0}]
									// [{"id":1,"url":"","weight":50,"countryIds":"","type":0},{"id":60025,"url":"http://104.250.136.138:3333/api/smartlink?s=1073&at=4&rt=api&s1={s1}&s2={s2}&s3={s3}&s4={s4}&s5={s5}","weight":50,"countryIds":"","type":0}]

									final String link = "http://"+IP+":3333/api/smartlink?s="+rs.getString("id")+"&at=4&rt=api&s1={s1}&s2={s2}&s3={s3}&s4={s4}&s5={s5}";
									String json = "";
									json = OM.toJOSN(new ArrayList<HashMap<String, Object>>(2){{
										add(new HashMap<String, Object>(5){{
											put("id", 1);
											put("url", "");
											put("weight", 100-sagacityWeight);
											put("countryIds", "");
											put("type", 0);
										}});
										add(new HashMap<String, Object>(5){{
											put("id", linkStartId[0]);
											put("url", link);
											put("weight", sagacityWeight);
											put("countryIds", "");
											put("type", 0);
										}});
									}}, false);

									RedisTool.set("smart_flow:"+linkStartId[0], link);
//									jedis.set("smart_flow:"+linkStartId[0], link);

									appSmartConfigSqls.add("update APP set "
											+ "SmartConfig = '" + json + "' "
											+ "where id = " + rs.getInt("id") + " and ("
													  + "smartConfig is null "
													  + "or smartConfig = '' "
													  + "or id in (557623,2707,917597,497624,557624,1967593,497623,2708,1967594,917595, 917594, 17670, 1277610, 407605, 407604, 1007612)"
													+ " )" );
									linkStartId[0]++;
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
