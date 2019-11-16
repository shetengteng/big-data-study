package com.stt.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

object Ch07_Window {
  def main(args: Array[String]): Unit = {

    var conf = new SparkConf().setMaster("local[2]").setAppName("Ch07_Window")
    // 3s读取一次
    val context: StreamingContext = new StreamingContext(conf, Seconds(3))
    // 监听指定的端口，3s读取一次数据
    // 返回接收器
    val dstream: ReceiverInputDStream[String] = context.socketTextStream("hadoop102", 9999)
    // 将读取的数据扁平化
    val wordStream: DStream[String] = dstream.flatMap(_.split(" "))

    val tupleDstream: DStream[(String, Int)] = wordStream.map(w => (w, 1))

    // 进行抽口逆向操作需要检查点
    context.sparkContext.setCheckpointDir("data/spark/streaming")

    val re: DStream[(String, Int)] = tupleDstream.reduceByKeyAndWindow(
      (v1:Int,v2:Int)=>(v1+v2), // 具体操作
      (v1:Int,v2:Int)=>(v1-v2), // 窗口的逆向处理函数，将之前的数据做减法处理，需要checkPoint支持
      Seconds(9), // 窗口大小
      Seconds(3) // 窗口每次滑动距离
    )

    re.print
    // 开启接收器
    context.start()
    // main的是driver，需要一直启动，等待接收器执行
    context.awaitTermination()
  }
}