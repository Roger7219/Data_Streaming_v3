package com.mobikok.ssp.data.streaming.util

import java.io.{File, FileInputStream, FileOutputStream, IOException}
import java.sql.{ResultSet, ResultSetMetaData, SQLException, Types}
import java.text.SimpleDateFormat
import java.util

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.cfg.MapperConfig
import com.fasterxml.jackson.databind.introspect.AnnotatedMethod
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper, PropertyNamingStrategy, SerializationFeature}
import com.mobikok.ssp.data.streaming.util.MySqlJDBCClientV2.Callback
import org.apache.commons.io.IOUtils
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.DecimalType.DoubleDecimal
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext, SparkSession}
import org.apache.spark.sql.types._

/**
  * Created by Administrator on 2017/6/20.
  */
object OM {

  val INDENT_OUTPUT_OBJECT_MAPPER = new ObjectMapper().setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).configure(SerializationFeature.INDENT_OUTPUT, true).disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
  val OBJECT_MAPPER = new ObjectMapper().setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).configure(SerializationFeature.INDENT_OUTPUT, false).disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)

  def toJOSN(o: Any) : String ={
    toJOSN(o, true)
  }
  def toJOSN(o: Any, indentOutput: Boolean) : String ={
    if(indentOutput) {
      INDENT_OUTPUT_OBJECT_MAPPER.writeValueAsString(o)
    }else {
      OBJECT_MAPPER.writeValueAsString(o)
    }
  }

  def toBean[T] (json: String, clazz: Class[T]) : T ={
    INDENT_OUTPUT_OBJECT_MAPPER.readValue(json, clazz)
  }

  def toBean[T] (json: String, clazz: TypeReference[T]) : T ={
    INDENT_OUTPUT_OBJECT_MAPPER.readValue(json, clazz)
  }

  def convert[T] (o: Object, clazz: TypeReference[T]) : T ={
    INDENT_OUTPUT_OBJECT_MAPPER.convertValue(o, clazz)
  }

  def convert[T] (o: Object, clazz: Class[T]) : T ={
    INDENT_OUTPUT_OBJECT_MAPPER.convertValue(o, clazz)
  }


  def parseStructType(rs: ResultSet): StructType ={
    var structType = new StructType
    val md = rs.getMetaData
    val cs = md.getColumnCount
    val fs = Seq[StructField]()
    var i = 1
    while (i <= cs) {
      val n = md.getColumnLabel(i)
      val t = md.getColumnType(i)

      if (Types.TINYINT == t) {
        structType = structType.add(n, IntegerType)
      }
      else if (Types.SMALLINT == t) {
        structType = structType.add(n, IntegerType)
      }
      else if (Types.INTEGER == t) {
        structType = structType.add(n, IntegerType)
      }
      else if (Types.BIGINT == t) {
        structType = structType.add(n, LongType)
      }
      else if (Types.FLOAT == t || Types.REAL == t) {
        structType = structType.add(n, FloatType)
      }
      else if (Types.DOUBLE == t) {
        structType = structType.add(n, DoubleType)
      }
      else if (Types.DATE == t) {
        structType = structType.add(n, DateType)
      }
      else if (Types.TIME == t) {
        structType = structType.add(n, DateType)
      }
      else if (Types.TIMESTAMP == t) {
        structType = structType.add(n, TimestampType)
      }
      else if (Types.VARCHAR == t) {
        structType = structType.add(n, StringType)
      }
      else if(Types.LONGVARCHAR == t) {
        structType = structType.add(n, StringType)
      }
      else if(Types.LONGNVARCHAR == t) {
        structType = structType.add(n, StringType)
      }
      else if(Types.NCHAR  == t) {
        structType = structType.add(n, StringType)
      }
      else if(Types.NVARCHAR == t) {
        structType = structType.add(n, StringType)
      }
      else if (Types.CHAR == t) {
        structType = structType.add(n, StringType)
      }
      else if(Types.DECIMAL == t) {
        structType = structType.add(n, DecimalType(30, 15))
      }
      else {
        throw new RuntimeException("Undefined can handle column type: "
          + md.getColumnTypeName(i) +" ("+t+")" + " name: " + md.getColumnName(i) + " label: " + md.getColumnLabel(i))
      }
      i += 1
    }
    structType
  }


  private val LOG: org.apache.log4j.Logger = org.apache.log4j.Logger.getLogger(OM.getClass)

  private val LOWER_CASE_OBJECT_MAPPER: ObjectMapper = new ObjectMapper().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES).setPropertyNamingStrategy(new PropertyNamingStrategy() {
    override def nameForSetterMethod (config: MapperConfig[_], method: AnnotatedMethod, defaultName: String): String = defaultName.toLowerCase
  })

  def readBeanFromJSONFile[T] (filePath: String, typeReference: TypeReference[T]): T = try {
    val f: File = new File(filePath)
    if (!f.exists) throw new RuntimeException("JSON File '" + filePath + "' not exists !!")
    val lines: util.List[_] = IOUtils.readLines(new FileInputStream(filePath))
    val buff: StringBuffer = new StringBuffer
    import scala.collection.JavaConversions._
    for (s <- lines) {
      buff.append("\n").append(s)
    }
    LOG.warn("readBeanFromJSONFile: \n" + buff.toString)
    val json: String = buff.toString.replace("\n", "").replaceAll("	", "    ")
    OM.toBean(json, typeReference)
  } catch {
    case e: Exception =>
      throw new RuntimeException("Read JSON file '" + filePath + "' fail: ", e)
  }

  def writeBeanToJSONFile (filePath: String, bean: Any): Unit = {
    try {
      val json: String = OM.toJOSN(bean)
      val f: File = new File(filePath)
      if (!f.exists) f.createNewFile
      IOUtils.write(json, new FileOutputStream(f))
    } catch {
      case e: IOException =>
        throw new RuntimeException(e)
    }
  }

  def assembleAsFromLowerCaseFieldName[T] (rs: ResultSet, beanClass: Class[T]): util.List[T] = {
    val result: util.List[T] = new util.ArrayList[T]
    try
        while (rs.next) {
          val o: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef]
          val md: ResultSetMetaData = rs.getMetaData
          val c: Int = md.getColumnCount
          var i: Int = 1
          while (i <= c) {
            val n: String = rs.getMetaData.getColumnLabel(i)
            o.put(n, rs.getObject(n))

            i += 1;
          }
          result.add(LOWER_CASE_OBJECT_MAPPER.convertValue(o, beanClass))
        }
    catch {
      case e: SQLException =>
        throw new RuntimeException("遍历JDBC ResultSet异常：", e)
    }
    result
  }

  import scala.collection.JavaConversions._

  def assembleAsDataFrame (rs: ResultSet, sqlContext: SQLContext): DataFrame = {
    import sqlContext.implicits._

    val rows: java.util.List[Row] = assembleAsRow(rs)

    val md = rs.getMetaData
    val c = md.getColumnCount
    val fs = new Array[String](c)
    var i = 1
    while (i <= c) {
      fs(i - 1) = md.getColumnLabel(i)
      i += 1
    }
    val sc = parseStructType(rs)
    sqlContext.createDataFrame(rows, sc)
  }

// TEST
//  case class Person(name:String,age:Int)
//  def rddToDFCase(sparkSession : SparkSession): Unit = {
//    //导入隐饰操作，否则RDD无法调用toDF方法
//    import sparkSession.implicits._
//    val peopleRDD = sparkSession.sparkContext
//      .textFile("file:/E:/scala_workspace/z_spark_study/people.txt",2)
//      .map( x => x.split(",")).map( x => /*Person*/(x(0),x(1).trim().toInt))
//
//    peopleRDD.toDF()
//  }


  def assembleAsRow (rs: ResultSet): java.util.List[Row] = {
    val result = new java.util.ArrayList[Row]()
    try
        while (rs.next) {
//          val o: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef]
          val md: ResultSetMetaData = rs.getMetaData
          val c: Int = md.getColumnCount
          var i: Int = 1
          var row = Row()
          val fs = new Array[Any](c)
          while (i <= c) {
            val n: String = rs.getMetaData.getColumnLabel(i)
            //o.put(n, rs.getObject(n))
//            row :+= rs.getObject(n)
            fs(i - 1) = rs.getObject(i)

            i += 1;
          }
          result.add(Row(fs:_*))
//          result.add(Row.fromSeq(seq))
//          result.add(OBJECT_MAPPER.convertValue(o, beanClass))
        }

    catch {
      case e: SQLException =>
        throw new RuntimeException("遍历JDBC ResultSet异常：", e)
    }
    result
  }

  def assembleAs[T] (rs: ResultSet, beanClass: Class[T]): util.List[T] = {
    val result: util.List[T] = new util.ArrayList[T]
    try
        while (rs.next) {
          val o: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef]
          val md: ResultSetMetaData = rs.getMetaData
          val c: Int = md.getColumnCount
          var i: Int = 1
          while (i <= c) {
            val n: String = rs.getMetaData.getColumnLabel(i)
            o.put(n, rs.getObject(n))

            i += 1;
          }
          result.add(OBJECT_MAPPER.convertValue(o, beanClass))
        }

    catch {
      case e: SQLException =>
        throw new RuntimeException("遍历JDBC ResultSet异常：", e)
    }
    result
  }

  def assembleAsFromLowerCaseFieldName[T] (rs: ResultSet, beanCalss: TypeReference[T]): util.List[T] = {
    val result: util.List[T] = new util.ArrayList[T]
    try
        while (rs.next) {
          val o: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef]
          val md: ResultSetMetaData = rs.getMetaData
          val c: Int = md.getColumnCount
          var i: Int = 1
          while (i <= c) {
            val n: String = rs.getMetaData.getColumnLabel(i)
            o.put(n, rs.getObject(n))

            i += 1;
          }
          result.add(LOWER_CASE_OBJECT_MAPPER.convertValue(o, beanCalss).asInstanceOf[T])
        }
    catch {
      case e: SQLException =>
        throw new RuntimeException("遍历JDBC ResultSet异常：", e)
    }
    result
  }
}
