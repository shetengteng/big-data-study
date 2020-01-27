package com.stt.spark.dw.realtime.util

import java.util.ResourceBundle

import com.alibaba.fastjson.JSON

object ConfigurationUtil {

  // 之前做国际化使用，现在可以用于读取properties文件
  private val rb = ResourceBundle.getBundle("config")

  /**
    * 依据key获取配置文件的value
    * @param key
    * @return
    */
  def getValueByKey(key: String): String = {
    // 当前线程环境的类加载器，一般是 应用类加载器
    // Thread.currentThread().getContextClassLoader.getResourceAsStream("config.properties")
    rb.getString(key)
  }

  def getValueByKeyFrom(config:String,key:String):String = {
    ResourceBundle.getBundle(config).getString(key)
  }

  def getCondValue(attr: String): String = {
    val json = ResourceBundle.getBundle("condition").getString("condition.params.json")
    JSON.parseObject(json).getString(attr)
  }

  def main(args: Array[String]): Unit = {
    println(ConfigurationUtil.getCondValue("startDate"))
  }

}