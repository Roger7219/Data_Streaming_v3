package com.mobikok.smartlink;

import io.codis.jodis.RoundRobinJedisPool;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import redis.clients.jedis.Jedis;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import io.codis.jodis.JedisResourcePool;
import io.codis.jodis.JedisResourcePool;

public class RedisTool {
	
	private static Log logger = LogFactory.getLog(RedisTool.class);
	
	private static JedisResourcePool jedisPool;
	public static final String ZK_ADDR ="104.250.141.178:2181,104.250.137.154:2181,104.250.128.138:2181";//"192.168.1.244:2181";//

	static {		
		try {
			jedisPool = RoundRobinJedisPool.create()
		        .curatorClient(ZK_ADDR, 30000).zkProxyDir("/zk/codis/db_kok_adv/proxy").build();
			
			jedisPool.getResource();
		}catch(Throwable t) {
			t.printStackTrace();
		}
	}
	/**
	 * 插入key-val键值数据
	 */
	public static void set(String key, String val) {
		long start = System.currentTimeMillis();
		try (Jedis jedis = jedisPool.getResource()) {			
		    jedis.set(key, val);		    
		}
		catch(Exception ex){
			ex.printStackTrace();
			//throw ex;
			return;
		}
//		logger.info("set method time:"+ (System.currentTimeMillis()-start) + "ms");
	}

	/**
	 * 获取key-val数据
	 */
	public static String get(String key) {
		long start = System.currentTimeMillis();
		try (Jedis jedis = jedisPool.getResource()) {			
		    String val = jedis.get(key);
//		    logger.info("get method time:"+ (System.currentTimeMillis()-start) + "ms");
		    return val;		    
		}
		catch(Exception ex){
			ex.printStackTrace();
		}
		return "";
	}
	

	/**
	 * 设置key的过期时间
	 */
	public static long expire(String key, int seconds) {
		long start = System.currentTimeMillis();
		try (Jedis jedis = jedisPool.getResource()) {	
			long rv = jedis.expire(key, seconds);
//			logger.info("expire method time:"+ (System.currentTimeMillis()-start) + "ms");
			return rv;
		}
		catch(Exception ex){
			ex.printStackTrace();
			return 0;
		}
	}
	public static long del(String key) {
		try (Jedis jedis = jedisPool.getResource()) {	
			return jedis.del(key);
		}
		catch(Exception ex){
			ex.printStackTrace();
			return 0;
		}
	}

	/**
	 * 往list中插入数据，对值不排重
	 */
	public static long lpush(String key, String jsonObject) {
		long start = System.currentTimeMillis();
		try (Jedis jedis = jedisPool.getResource()) {			
		    long rv = jedis.lpush(key, jsonObject);
//		    logger.info("lpush string method time:"+ (System.currentTimeMillis()-start) + "ms");
		    return rv;
		}
		catch(Exception ex){
			ex.printStackTrace();
//			throw ex;
			return 0;
		}
	}
	

	public static long lpush(String key, List<String> records) {
		long start = System.currentTimeMillis();
		long rv = 0;
		try (Jedis jedis = jedisPool.getResource()) {	
			rv = jedis.lpush(key, records.toArray(new String[0]));
		}
		catch(Exception ex){
			ex.printStackTrace();
			return 0;
		}
//		logger.info("lpush string[] method time:"+ (System.currentTimeMillis()-start) + "ms");
		return rv;
	}

	/**
	 * 往集合中插入数据，对值有排重
	 */
	public static long sadd(String key, String val) {
		try (Jedis jedis = jedisPool.getResource()) {	
		    return jedis.sadd(key, val);
		}
		catch(Exception ex){
			ex.printStackTrace();
//			throw ex;
			return 0;
		}
		
	}
	
	public static long scard(String key) {
		try (Jedis jedis = jedisPool.getResource()) {	
		    return jedis.scard(key);
		}
		catch(Exception ex){
			ex.printStackTrace();
//			throw ex;
			return 0;
		}
	}
	/**
	 * 获取某集合中的成员
	 */
	public static Set<String> smembers(String key) {
		try (Jedis jedis = jedisPool.getResource()) {	
			return jedis.smembers(key);		    
		}
		catch(Exception ex){
			ex.printStackTrace();
//			throw ex;
			return new HashSet<String>();
		}
	}
	
	public static boolean sismember(String key,String value) {
		try (Jedis jedis = jedisPool.getResource()) {	
			return jedis.sismember(key, value);
		}
		catch(Exception ex){
			ex.printStackTrace();
//			throw ex;
			return false;
		}
	}
	/**
	 * 往有序集合中插入数据，对值有排重-对分值进行了排序
	 */
	public static long zadd(String key, double score, String val) {
		try (Jedis jedis = jedisPool.getResource()) {	
			return jedis.zadd(key, score, val);	    
		}
		catch(Exception ex){
			ex.printStackTrace();
//			throw ex;
			return 0;
		}
	}
	
	public static double zincrby(String key,double score,String val) {
		try (Jedis jedis = jedisPool.getResource()) {	
			return jedis.zincrby(key, score, val);	    
		}
		catch(Exception ex){
			ex.printStackTrace();
//			throw ex;
			return 0;
		}
	}
	
	
	/**
	 * 获取根据分值倒序的区间数据
	 */
	public static Set<String> zrevrange(String key, long start, long end) {
		try (Jedis jedis = jedisPool.getResource()) {	
			return jedis.zrevrange(key, start, end);	    
		}
		catch(Exception ex){
			ex.printStackTrace();
//			throw ex;
			return new HashSet<String>();
		}
	}
	
	public static double zscore(String key,String member) {
		try (Jedis jedis = jedisPool.getResource()) {	
			return jedis.zscore(key, member);
		}catch(Exception ex){
//			ex.printStackTrace();
//			throw ex;
			return 0;
		}
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
//		RedisTool.set("test", "123");
		String r = RedisTool.get("dsp_allconfig");
		//ab56f827-6c84-427d-9c50-e5979fb5ff9b
//		double score = RedisTool.zscore("ssp_campaigndayinfo:20160907", "3");
		System.out.println(r);
	}

}
