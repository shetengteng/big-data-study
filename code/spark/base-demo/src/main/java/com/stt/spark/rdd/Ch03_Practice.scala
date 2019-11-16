package com.stt.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Ch03_Practice {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Practice")
    val sc: SparkContext = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile("data/spark/Ch03/input.txt")

    // 将每一行数据转换为一个元组对象((P,A),1)
    // 1516609143867 6 7 64 16 => ((6,16),1)
    val PAToOne: RDD[((String, String), Int)] = lines.map(line => {
      val words: Array[String] = line.split(" ")
      val province = words(1)
      val advertisement = words(4)
      ((province, advertisement), 1)
    })
    // 计算广告点击总个数((P,A),SUM)
    val PAToSum: RDD[((String, String), Int)] = PAToOne.reduceByKey(_+_)
    // 将((P,A),SUM) 转换为(P,(A,SUM))
    val PToASum: RDD[(String, (String, Int))] = PAToSum.map(item => {
      (item._1._1, (item._1._2, item._2))
    })
    // 对(P,(A,SUM))进行分组 => (P,[(A1,SUM1),(A2,SUM2)])
    val pGroup: RDD[(String, Iterable[(String, Int)])] = PToASum.groupByKey()
    // 对分组信息求top3
    val top3: RDD[(String, List[(String, Int)])] = pGroup.mapValues(item=>{
      item.toList.sortWith((item1,item2)=> item1._2 > item2._2).take(3)
    })

    var result = top3.collect()
    // 对结果进行输出
//    top3.collect.foreach(println)
//    result.foreach(item=>{
//      item._2.map{
//        case (p,sum) => println(item._1+"-"+p+"-"+sum)
//      }
//    })

    result.foreach(item=>{
      item._2.foreach(subItem=>{
        println(item._1+"-"+subItem._1+"-"+subItem._2)
      })
    })

    sc.stop()
  }
}
