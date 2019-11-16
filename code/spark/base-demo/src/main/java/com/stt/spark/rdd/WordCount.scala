package com.stt.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
    Driver
       在main中创建了SparkContext对象，就是Driver类

 */
object WordCount {
  def main(args: Array[String]): Unit = {

    var path = args(0)
//    var path = "data/spark/wordCount"
    // 创建spark配置对象
    val conf: SparkConf = new SparkConf()
      .setMaster("local").setAppName("WordCount")
    // 创建spark上下文环境
    val sc: SparkContext = new SparkContext(conf)
    // 从开发工具的根目录读取本地文件
    val lines: RDD[String] = sc.textFile(path)
    // 将获取文件的每一行内容扁平化为单词
    val words: RDD[String] = lines.flatMap(_.split(" "))
    // 将每一单词转换为计算的元祖
    val wordAndOnes: RDD[(String, Int)] = words.map((_,1))
    // 根据元组的第一个元素进行聚合统计
    // 将每个Executor中的结果进行返回汇聚
    val wordAndSum: RDD[(String, Int)] = wordAndOnes.reduceByKey(_+_)
    // 将统计结果收集到内存中展示
    val result: Array[(String, Int)] = wordAndSum.collect
    // 打印
    result.foreach(println(_))
    // 关闭
    sc.stop()

  }
}