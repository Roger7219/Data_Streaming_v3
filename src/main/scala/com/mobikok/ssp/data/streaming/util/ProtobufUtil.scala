package com.mobikok.ssp.data.streaming.util

import com.google.protobuf.Message
import com.googlecode.protobuf.format.JsonFormat

/**
  * Created by Administrator on 2017/7/11.
  */
object ProtobufUtil {

  def protobufToJSON(protoMessageClass: Class[Message] , protoMsg: Array[Byte]): String ={
    //反射调用静态方法
    val m = protoMessageClass.getMethod("parseFrom", classOf[Array[Byte]]).invoke(null, protoMsg).asInstanceOf[Message]
    JsonFormat.printToString(m)
  }
}
