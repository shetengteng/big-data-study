package com.stt.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}

import scala.collection.mutable


object Ch03_QueueSource {
  def main(args: Array[String]): Unit = {

    var conf = new SparkConf().setMaster("local[*]").setAppName("Ch03_QueueSource")
    // 5s读取一次
    val context: StreamingContext = new StreamingContext(conf,Seconds(5))

    // 创建RDD队列
    var queueSource = new mutable.Queue[RDD[Int]]()

//    val dstream: InputDStream[Int] = context.queueStream(queueSource)
    val dstream: InputDStream[Int] = context.queueStream(queueSource,false)
    // 求和
    dstream.reduce(_ + _).print()
    // 开启接收器
    context.start()

    // 循环创建并向RDD队列中放入RDD
    for(i<- 1 to 5){
      // val value: RDD[Int] = context.sparkContext.makeRDD(1 to 5)
      // 等价于
      val value: RDD[Int] = context.sparkContext.makeRDD(Array(1,2,3,4,5))
      queueSource += value
//      Thread.sleep(2000)
    }

    // main的是driver，需要一直启动，等待接收器执行
    context.awaitTermination()
  }
}
