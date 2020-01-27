package com.stt.project

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 清理
  * 1)/user/hive/warehouse/ods.db/origin_user_behavior/${day}目录中，需要用SparkCore将数据清洗
  * 清洗需求如下：
  * a)手机号脱敏：187xxxx2659
  * b)过滤重复行（重复条件，uid,event_key,event_time三者都相同即为重复）
  * c)最终数据保存到ods.user_behavior分区表，以dt（天）为分区条件，表的文件存储格式为ORC，数据总量为xxxx条
  */
object UserBehaviorCleaner {

  def main(args: Array[String]): Unit = {

    if(args.length !=2) {
      println("input or output miss")
      System.exit(1)
    }

    // 获取输入输出路径
    val inputPath=args(0)
    val outputPath=args(1)

//    val conf: SparkConf = new SparkConf().setAppName("UserBehaviorCleaner").setMaster("local[*]")
    val conf: SparkConf = new SparkConf().setAppName("UserBehaviorCleaner")
    val sc = new SparkContext(conf)

    // 通过输入路径获取RDD
    val eventRDD: RDD[String] = sc.textFile(inputPath)

    eventRDD.filter(event => checkEventValid(event)) // 验证数据有效性
      .map(event => maskPhone(event)) // 对手机号码进行脱敏
      .map(event => repairUsername(event)) // 修复username中带有\n导致的换行
      .coalesce(3) // 重分区，优化处理
      .saveAsTextFile(outputPath)

    sc.stop()
  }

  /**
    * 检验格式是否正确，只有17个字段才符合规则
    * @param event
    * @return
    */
  def checkEventValid(event: String): Boolean = {
    val fields: Array[String] = event.split("\t")
    return fields.length == 17
  }

  /**
    * 脱敏手机号
    * @param event
    * @return
    */
  def maskPhone(event: String): String = {
    val fields: Array[String] = event.split("\t")
    var phoneNum: String = fields(9)
    // 号码有效时进行数据处理
    if(phoneNum != "" && !"Null".equals(phoneNum)){
      fields(9) = new StringBuffer(phoneNum.substring(0,3))
              .append("xxx")
              .append(phoneNum.substring(7,11))
              .toString
    }
    fields.mkString("\t")
  }

  /**
    * username为用户自定义的，里面有要能存在"\n"，导致写入到HDFS时换行
    * @param event
    * @return
    */
  def repairUsername(event:String):String ={
    val fields: Array[String] = event.split("\t")
    val username: String = fields(1)
    if(username != null && username != ""){
      fields(1)=username.replace("\n","")
    }
    fields.mkString("\t")
  }
}
