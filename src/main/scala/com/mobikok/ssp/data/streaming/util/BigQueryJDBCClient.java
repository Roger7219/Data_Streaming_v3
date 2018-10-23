package com.mobikok.ssp.data.streaming.util;

import java.net.InetAddress;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.reflect.internal.Trees.Throw;


public class BigQueryJDBCClient {

//	static String OAuthKeyFile = "C:/Users/Administrator/Desktop/key.json";
//
//	private static String url ="jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;ProjectId=dogwood-seeker-182806;OAuthType=0;OAuthServiceAcctEmail=kairenlo@msn.cn;OAuthPvtKeyPath="+OAuthKeyFile+";Timeout=3600;";
//
	private String url = "";
	static DataSource dataSource;
	public BigQueryJDBCClient(final String url) {
		this.url = url;
		try{
			this.url =  url;
			com.simba.googlebigquery.jdbc42.DataSource dataSource = new com.simba.googlebigquery.jdbc42.DataSource();
			dataSource.setURL(url);
			this.dataSource = dataSource;
		}catch (Throwable e){
			LOG.error("Create BigQuery dataSource fail", e);
//			e.printStackTrace();
		}

	}

	private Logger LOG = LoggerFactory.getLogger(getClass());


	public Connection getConnection() throws Exception {
		Connection connection = null;
		connection = DriverManager.getConnection(url);
		return connection;
	}

	private Connection getConnectViaDS() throws Exception{
		Connection connection = null;
		connection = dataSource.getConnection();
		return connection;
	}

	public void executeBatch(String[] sqls) {
		long s = new Date().getTime();

		LOG.warn("Executing batch SQLs count: \n" + sqls.length);

		if (sqls.length == 0) {
			LOG.warn("Executed batch SQLs done", "No statement has been executed !!");
			return;
		}

		Boolean ac = null;
		Connection conn = null;
		Statement st = null;
		try {

			LOG.warn("BasicDataSource getConnection() starting");
			conn = getConnectViaDS();
			LOG.warn("BasicDataSource getConnection() done");
			ac = conn.getAutoCommit();

			conn.setAutoCommit(false);

			st = conn.createStatement();
			for (String x : sqls) {
				st.addBatch(x);
			}

			st.executeBatch();
			LOG.warn("BasicDataSource executeBatch() done, wait commit...");

			conn.commit();

			LOG.warn("Executed batch SQLs: \n" + Arrays.deepToString(sqls));

		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			if (ac != null && conn != null) {
				try {
					conn.setAutoCommit(ac);
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}

			if (st != null) {
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

		}
		LOG.warn("Executed SQL: \nUsing time: \n" + (new Date().getTime()-s)/1000 +"s" );
	}

	public void execute(String sql) {
		execute0(sql);
	}

	public <T> List<T> executeQuery(String sql, final Class<T> itemClazz) {
		return executeQuery0(sql, new Callback<List<T>>() {
			public List<T> onCallback(ResultSet rs) {
				return BeanUtil.assembleAs(rs, itemClazz);
			}
		});
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
			conn = getConnectViaDS();
			conn.setAutoCommit(true);
			st = conn.createStatement();
			rs = st.executeQuery(sql);
			return callback.onCallback(rs);

		} catch (Exception e) {
			String by = "";
			try {
				by = "on " + System.getProperty("user.name") + "@" + InetAddress.getLocalHost().getHostName() + "(" + InetAddress.getLocalHost().getHostAddress() + ")";
			}catch (Throwable t) { }
			throw new RuntimeException("Fail SQL "+by+" : \n" + sql + "\n", e);
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
			conn = getConnectViaDS();
			conn.setAutoCommit(true);
			st = conn.createStatement();
			st.execute(sql);
		} catch (Exception e) {
			e.printStackTrace();
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
