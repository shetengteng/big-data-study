package com.stt.spark.mall.common

object CommonUtil {

  /**
    * 判断字符串是否为空
    * @param str
    * @return
    */
  def isEmpty(str: String): Boolean = {
     str == null || "".equals(str.trim)
  }

}
