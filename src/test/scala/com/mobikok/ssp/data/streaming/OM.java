//package com.mobikok.ssp.data.streaming;
//
//import com.fasterxml.jackson.core.type.TypeReference;
//import com.fasterxml.jackson.databind.DeserializationFeature;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.fasterxml.jackson.databind.PropertyNamingStrategy;
//import com.fasterxml.jackson.databind.SerializationFeature;
//import com.fasterxml.jackson.databind.cfg.MapperConfig;
//import com.fasterxml.jackson.databind.introspect.AnnotatedMethod;
//import org.apache.commons.io.IOUtils;
//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Row;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.File;
//import java.io.FileInputStream;
//import java.io.FileOutputStream;
//import java.io.IOException;
//import java.sql.ResultSet;
//import java.sql.ResultSetMetaData;
//import java.sql.SQLException;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
///**
// * Created by Administrator on 2017/6/20.
// */
//public class OM {
//
//	private static Logger LOG = LoggerFactory.getLogger(OM.class);
//
//	private static ObjectMapper INDENT_OUTPUT_OBJECT_MAPPER = new ObjectMapper().configure(
//			SerializationFeature.INDENT_OUTPUT, true).disable(
//			DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
//
//	private static ObjectMapper LOWER_CASE_OBJECT_MAPPER = new ObjectMapper().disable(
//			DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
//			.setPropertyNamingStrategy(new PropertyNamingStrategy() {
//				public String nameForSetterMethod(MapperConfig<?> config,
//												  AnnotatedMethod method, String defaultName) {
//					return defaultName.toLowerCase();
//				}
//			});
//
//	private static ObjectMapper OBJECT_MAPPER = new ObjectMapper().disable(
//			DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
//
//	public static String toJOSN(Object o) {
//		try {
//			return INDENT_OUTPUT_OBJECT_MAPPER.writeValueAsString(o);
//		} catch (Exception e) {
//			throw new RuntimeException(e);
//		}
//	}
//
//	public static <T> T toBean(String json, Class<T> clazz) {
//		try {
//			return INDENT_OUTPUT_OBJECT_MAPPER.readValue(json, clazz);
//		} catch (Exception e) {
//			throw new RuntimeException(e);
//		}
//	}
//
//	public static <T> T toBean(String json, TypeReference<T> clazz) {
//		try {
//			return INDENT_OUTPUT_OBJECT_MAPPER.readValue(json, clazz);
//		} catch (Exception e) {
//			throw new RuntimeException(e);
//		}
//	}
//
//	public static <T> T convert(Object o, TypeReference<T> clazz) {
//		try {
//			return INDENT_OUTPUT_OBJECT_MAPPER.convertValue(o, clazz);
//		} catch (Exception e) {
//			throw new RuntimeException(e);
//		}
//	}
//
//	public static <T> T convert(Object o, Class<T> clazz) {
//		try {
//			return INDENT_OUTPUT_OBJECT_MAPPER.convertValue(o, clazz);
//		} catch (Exception e) {
//			throw new RuntimeException(e);
//		}
//	}
//
//	public static <T> T readBeanFromJSONFile(String filePath, TypeReference<T> typeReference){
//
//		try {
//
//			File f = new File(filePath);
//			if(!f.exists()) {
//				throw new RuntimeException("JSON File '"+filePath+"' not exists !!");
//			}
//
//			List<String> lines = IOUtils.readLines(new FileInputStream(filePath));
//
//			StringBuffer buff = new StringBuffer();
//			for(String s : lines) {
//				buff.append("\n").append(s);
//			}
//
//			LOG.warn("readBeanFromJSONFile: \n" +buff.toString());
//
//			String json = buff.toString().replace("\n", "").replaceAll("	", "    ");
//			return com.mobikok.ssp.data.streaming.util.OM.toBean(json, typeReference);
//
//		} catch (Exception e) {
//			throw new RuntimeException("Read JSON file '"+filePath+"' fail: ",e);
//		}
//
//	}
//
//	public static void writeBeanToJSONFile(String filePath, Object bean){
//
//		try {
//			String json = com.mobikok.ssp.data.streaming.util.OM.toJOSN(bean);
//			File f = new File(filePath);
//			if(!f.exists()) {
//				f.createNewFile();
//			}
//			IOUtils.write(json, new FileOutputStream(f));
//
//		} catch (IOException e) {
//			throw new RuntimeException(e);
//		}
//
//	}
//
//	public static <T> List<T> lowerCaseFieldNameAssembleAs(ResultSet rs, Class<T> calzz) {
//
//		List<T> result = new ArrayList<T>();
//		try {
//			while (rs.next()) {
//				Map<String, Object> o = new HashMap<String, Object>();
//
//				ResultSetMetaData md = rs.getMetaData();
//				int c = md.getColumnCount();
//
//				for (int i = 1; i <= c; i++) {
//					String n = rs.getMetaData().getColumnLabel(i);
//					o.put(n, rs.getObject(n));
//				}
//				result.add(LOWER_CASE_OBJECT_MAPPER.convertValue(o, calzz));
//			}
//		} catch (SQLException e) {
//			throw new RuntimeException("遍历JDBC ResultSet异常：", e);
//		}
//
//		return result;
//	}
//
//	public static <T> List<T> assembleAs(ResultSet rs, Class<T> calzz) {
//
//		List<T> result = new ArrayList<T>();
//		try {
//			while (rs.next()) {
//				Map<String, Object> o = new HashMap<String, Object>();
//
//				ResultSetMetaData md = rs.getMetaData();
//				int c = md.getColumnCount();
//
//				for (int i = 1; i <= c; i++) {
//					String n = rs.getMetaData().getColumnLabel(i);
//					o.put(n, rs.getObject(n));
//				}
//				result.add(OBJECT_MAPPER.convertValue(o, calzz));
//			}
//		} catch (SQLException e) {
//			throw new RuntimeException("遍历JDBC ResultSet异常：", e);
//		}
//
//		return result;
//	}
//
////	public static Dataset<Row> assembleAsDataFrame(ResultSet rs) {
////
////		try {
////			ResultSetMetaData md = rs.getMetaData();
////			int cs = md.getColumnCount();
////
////			for(int i = 0; i < cs; i++) {
////				String n = md.getColumnName(i);
////				int t = md.getColumnType(i);
////				if(Types.INTEGER == t) {
////
////				}
////
////
////				StructField("timestamp", LongType) :: Nil
////			}
////
////			list = assembleAs(rs, List.class);
////		} catch (SQLException e) {
////			e.printStackTrace();
////		}
////
////	}
//
//	public static <T> List<T> lowerCaseFieldNameAssembleAs(ResultSet rs, TypeReference<T> calzz) {
//
//		List<T> result = new ArrayList<T>();
//		try {
//			while (rs.next()) {
//				Map<String, Object> o = new HashMap<String, Object>();
//
//				ResultSetMetaData md = rs.getMetaData();
//				int c = md.getColumnCount();
//
//				for (int i = 1; i <= c; i++) {
//					String n = rs.getMetaData().getColumnLabel(i);
//					o.put(n, rs.getObject(n));
//				}
//				result.add((T)LOWER_CASE_OBJECT_MAPPER.convertValue(o, calzz));
//			}
//		} catch (SQLException e) {
//			throw new RuntimeException("遍历JDBC ResultSet异常：", e);
//		}
//
//		return result;
//	}
//
//}
