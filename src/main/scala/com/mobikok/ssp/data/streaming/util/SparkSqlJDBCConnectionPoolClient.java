package com.mobikok.ssp.data.streaming.util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;

import org.apache.commons.dbcp.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Administrator on 2018/3/5.
 */
public class SparkSqlJDBCConnectionPoolClient {

	private String url;
	private String user;
	private String password;

	private Logger LOG = LoggerFactory.getLogger(getClass());

	private String driver = "org.apache.hive.jdbc.HiveDriver";
	private BasicDataSource basicDataSource = null;

	public SparkSqlJDBCConnectionPoolClient(){
		this("jdbc:hive2://node15:10016/default","", "");
	}
	public SparkSqlJDBCConnectionPoolClient(String url){
		this(url,"", "");
	}
	public SparkSqlJDBCConnectionPoolClient(String url, String user, String password) {
		this.url = url;
		this.user = user;
		this.password = password;
		init();
	}
	
	public void init(){
		try {
			Class.forName(driver);
		} catch (Exception e) {
			throw new RuntimeException("加载SparkSql Driver类异常", e);
		}
		basicDataSource = new BasicDataSource();
		basicDataSource.setInitialSize(1);
//		basicDataSource.setMinIdle(25);
		basicDataSource.setMaxActive(50);
//		basicDataSource.setMaxWait(10000);
//		basicDataSource.setLogAbandoned(true);
//		basicDataSource.setTestWhileIdle(true);
		basicDataSource.setValidationQuery("show databases");
//		basicDataSource.setTimeBetweenEvictionRunsMillis(60000);
//		basicDataSource.setNumTestsPerEvictionRun(50);
		basicDataSource.setDriverClassName(driver);
		basicDataSource.setUrl(this.url);
//		basicDataSource.setRemoveAbandoned(true);
//		basicDataSource.setRemoveAbandonedTimeout(1800);
		basicDataSource.setUsername(user);
		basicDataSource.setPassword(password);
		basicDataSource.setDefaultAutoCommit(true);
		basicDataSource.setTestOnBorrow(true);
	}

	public void execute(String sql) {
		execute0(sql);
	}

	public <T> T executeQuery(String sql, Callback<T> callback) {
		return executeQuery0(sql, callback); 
	}
	
	public interface Callback<T>{
		public T onCallback(ResultSet rs);
	}

	public <T> T executeQuery0(String sql, Callback<T> callback) {
		long s = new Date().getTime();
		Statement st = null;
		Connection conn = null;
		ResultSet rs = null;
		try {
			LOG.warn("Executing SQL: \n" + sql);
			LOG.warn("BasicDataSource executeQuery getConnection() starting");
			conn = basicDataSource.getConnection();
			
			LOG.warn("BasicDataSource executeQuery getConnection() done");
			st = conn.createStatement();
			
			st.execute("set spark.sql.shuffle.partitions=1");
			st.execute("set hive.exec.dynamic.partition.mode=nonstrict");
			rs = st.executeQuery(sql);
			return callback.onCallback(rs);
			
		} catch (Exception e) {
			//重新初始化
			init();
			throw new RuntimeException("Fail SQL: \n" + sql + "\n", e);
		} finally {
			if(st != null) {
				try {
					st.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if(rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if( conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
				
			}
			LOG.warn("Executed SQL: \n" +  sql + "\n\nUsing time: \n" + (new Date().getTime()-s)/1000 +"s" );
		}
	}

	public void execute0(String sql) {
		long s = new Date().getTime();
		Statement st = null;
		Connection conn = null;
		try {
			LOG.warn("Executing SQL: \n" + sql);
			LOG.warn("BasicDataSource execute getConnection() starting");
			conn = basicDataSource.getConnection();
			LOG.warn("BasicDataSource execute getConnection() done");
			st = conn.createStatement();
			
			st.execute("set spark.sql.shuffle.partitions=1");
			st.execute("set hive.exec.dynamic.partition.mode=nonstrict");
			st.execute(sql);
		} catch (Exception e) {
			//重新初始化
			init();
			throw new RuntimeException("Fail SQL: " + sql + "\n", e);
		} finally {
			if(st != null) {
				try {
					st.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if(conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			LOG.warn("Executed SQL: \n" + sql + "\n\nUsing time: \n" + (new Date().getTime()-s));
		}
	}
	
	
}
