package com.mobikok.ssp.data.streaming.util;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Created by Administrator on 2017/8/21.
 */
public class CodisClient {
    private static JedisPool pool = null;

    /**
     * 构建redis连接池
     *
     * @param ip
     * @param port
     * @return JedisPool
     */
    public static JedisPool getPool() {
        if (pool == null) {
            JedisPoolConfig config = new JedisPoolConfig();
            //最大连接数
            config.setMaxTotal(50);
            //最大空闲连接数
            config.setMaxIdle(10);
            //每次释放连接的最大数目
            config.setNumTestsPerEvictionRun(1024);
            //释放连接的扫描间隔（毫秒）
            config.setTimeBetweenEvictionRunsMillis(30000);
            //连接最小空闲时间
            config.setMinEvictableIdleTimeMillis(1800000);
            //连接空闲多久后释放, 当空闲时间>该值 且 空闲连接>最大空闲连接数 时直接释放
            config.setSoftMinEvictableIdleTimeMillis(10000);
            //获取连接时的最大等待毫秒数,小于零:阻塞不确定的时间,默认-1
            config.setMaxWaitMillis(1500);
            //在获取连接的时候检查有效性, 默认false
            config.setTestOnBorrow(false);
            //在空闲时检查有效性, 默认false
            config.setTestWhileIdle(true);
            //连接耗尽时是否阻塞, false报异常,ture阻塞直到超时, 默认true
            config.setBlockWhenExhausted(false);

            //10s超長時間
            pool = new JedisPool(config, "127.0.0.1", 6767, 10000);

        }
        return pool;
    }

    /**
     * 返回连接池
     *
     * @param pool
     * @param redis
     */
    public static void returnResource(JedisPool pool, Jedis redis) {
        if (redis != null) {
            pool.returnResource(redis);
        }
    }

    /**
     * codis读取数据
     *
     * @param key
     * @return
     */
    public static String get(String key){
        String value = null;

        JedisPool pool = null;
        Jedis jedis = null;
        try {
            pool = getPool();
            jedis = pool.getResource();
            value = jedis.get(key);

        } catch (Exception e) {
            //释放redis对象
            pool.returnBrokenResource(jedis);
            e.printStackTrace();
        } finally {
            //返还到连接池
            returnResource(pool, jedis);
        }
        return value;
    }

    /**
     * codis写入数据
     * @param key
     * @param value
     */
    public static void set(String key, String value){
        JedisPool pool = null;
        Jedis jedis = null;
        try {
            pool = getPool();
            jedis = pool.getResource();
            jedis.set(key, value);

        } catch (Exception e) {
            //释放redis对象
            pool.returnBrokenResource(jedis);
            e.printStackTrace();
        } finally {
            //返还到连接池
            returnResource(pool, jedis);
        }
    }

}
