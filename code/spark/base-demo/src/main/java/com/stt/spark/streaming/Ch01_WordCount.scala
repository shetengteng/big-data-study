package com.stt.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Ch01_WordCount {
  def main(args: Array[String]): Unit = {

    var conf = new SparkConf().setMaster("local[2]").setAppName("Ch01_WordCount")
    // 3s读取一次
    val context: StreamingContext = new StreamingContext(conf,Seconds(3))
    // 监听指定的端口，3s读取一次数据
    // 返回接收器
    val dstream: ReceiverInputDStream[String] = context.socketTextStream("hadoop102",9999)
    // 将读取的数据扁平化
    val wordStream: DStream[String] = dstream.flatMap(_.split(" "))

    val tupleDstream: DStream[(String, Int)] = wordStream.map(w=>(w,1))

    val result: DStream[(String, Int)] = tupleDstream.reduceByKey(_ + _)

    result.print

    // 开启接收器
    context.start()

    // main的是driver，需要一直启动，等待接收器执行
    context.awaitTermination()
  }
}
