package com.mobikok.ssp.data.streaming.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.cfg.MapperConfig;
import com.fasterxml.jackson.databind.introspect.AnnotatedMethod;

public class BeanUtil {

	private static Logger LOG = LoggerFactory.getLogger(BeanUtil.class);

	private static ObjectMapper LOWER_CASE_OBJECT_MAPPER = new ObjectMapper().disable(
			DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
			.setPropertyNamingStrategy(new PropertyNamingStrategy() {
				public String nameForSetterMethod(MapperConfig<?> config,
						AnnotatedMethod method, String defaultName) {
					return defaultName.toLowerCase();
				}
			});

	private static ObjectMapper OBJECT_MAPPER = new ObjectMapper().disable(
			DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);


	public static <T> T readBeanFromJSONFile(String filePath, TypeReference<T> typeReference){

		try {

			File f = new File(filePath);
			if(!f.exists()) {
				throw new RuntimeException("JSON File '"+filePath+"' not exists !!");
			}

			List<String> lines = IOUtils.readLines(new FileInputStream(filePath));

			StringBuffer buff = new StringBuffer();
			for(String s : lines) {
				buff.append("\n").append(s);
			}

			LOG.warn("readBeanFromJSONFile: \n" +buff.toString());

			String json = buff.toString().replace("\n", "").replaceAll("	", "    ");
			return OM.toBean(json, typeReference);

		} catch (Exception e) {
			throw new RuntimeException("Read JSON file '"+filePath+"' fail: ",e);
		}

	}

	public static void writeBeanToJSONFile(String filePath, Object bean){

		try {
			String json = OM.toJOSN(bean);
			File f = new File(filePath);
			if(!f.exists()) {
				f.createNewFile();
			}
			IOUtils.write(json, new FileOutputStream(f));

		} catch (IOException e) {
			throw new RuntimeException(e);
		}

	}

	public static <T> List<T> lowerCaseFieldNameAssembleAs(ResultSet rs, Class<T> calzz) {

		List<T> result = new ArrayList<T>();
		try {
			while (rs.next()) {
				Map<String, Object> o = new HashMap<String, Object>();

				ResultSetMetaData md = rs.getMetaData();
				int c = md.getColumnCount();

				for (int i = 1; i <= c; i++) {
					String n = rs.getMetaData().getColumnLabel(i);
					o.put(n, rs.getObject(n));
				}
				result.add(LOWER_CASE_OBJECT_MAPPER.convertValue(o, calzz));
			}
		} catch (SQLException e) {
			throw new RuntimeException("遍历JDBC ResultSet异常：", e);
		}

		return result;
	}

	public static <T> List<T> assembleAs(ResultSet rs, Class<T> calzz) {

		List<T> result = new ArrayList<T>();
		try {
			while (rs.next()) {
				Map<String, Object> o = new HashMap<String, Object>();

				ResultSetMetaData md = rs.getMetaData();
				int c = md.getColumnCount();

				for (int i = 1; i <= c; i++) {
					String n = rs.getMetaData().getColumnLabel(i);
					o.put(n, rs.getObject(n));
				}
				result.add(OBJECT_MAPPER.convertValue(o, calzz));
			}
		} catch (SQLException e) {
			throw new RuntimeException("遍历JDBC ResultSet异常：", e);
		}

		return result;
	}

	public static <T> List<T> lowerCaseFieldNameAssembleAs(ResultSet rs, TypeReference<T> calzz) {

		List<T> result = new ArrayList<T>();
		try {
			while (rs.next()) {
				Map<String, Object> o = new HashMap<String, Object>();

				ResultSetMetaData md = rs.getMetaData();
				int c = md.getColumnCount();

				for (int i = 1; i <= c; i++) {
					String n = rs.getMetaData().getColumnLabel(i);
					o.put(n, rs.getObject(n));
				}
				result.add((T)LOWER_CASE_OBJECT_MAPPER.convertValue(o, calzz));
			}
		} catch (SQLException e) {
			throw new RuntimeException("遍历JDBC ResultSet异常：", e);
		}

		return result;
	}



}
