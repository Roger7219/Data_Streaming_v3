package com.mobikok.ssp.data.streaming.util

object ExceptionUtil {

  def getStackTraceMessage(e: Exception): String = {
    val elements = e.getStackTrace
    val exceptionMessage = new StringBuilder
    exceptionMessage.append(e.getClass.getName).append(": ").append(e.getMessage).append("\n")
    elements.foreach{ element =>
      exceptionMessage.append("    at ").append(element.getClassName).append(".").append(element.getMethodName).append("(").append(element.getFileName)
      if (element.getLineNumber != -1) exceptionMessage.append(":").append(element.getLineNumber)
      exceptionMessage.append(")\n")
    }
    exceptionMessage.toString
  }

}
