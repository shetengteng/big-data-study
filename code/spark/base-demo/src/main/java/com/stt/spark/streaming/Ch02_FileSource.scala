package com.stt.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

object Ch02_FileSource {
  def main(args: Array[String]): Unit = {

    var conf = new SparkConf().setMaster("local[*]").setAppName("Ch02_FileSource")
    // 5s读取一次
    val context: StreamingContext = new StreamingContext(conf,Seconds(5))
    // 监听指定的文件夹，5s读取一次数据
    val dstream: DStream[String] = context.textFileStream("hdfs://hadoop102:9000/fileStream")
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
