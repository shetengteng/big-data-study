package com.stt.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

object Ch06_StateRDD_updateStateByKey {
  def main(args: Array[String]): Unit = {

    var conf = new SparkConf().setMaster("local[2]").setAppName("Ch06_StateRDD")
    // 3s读取一次
    val context: StreamingContext = new StreamingContext(conf, Seconds(3))
    // 监听指定的端口，3s读取一次数据
    // 返回接收器
    val dstream: ReceiverInputDStream[String] = context.socketTextStream("hadoop102", 9999)
    // 将读取的数据扁平化
    val wordStream: DStream[String] = dstream.flatMap(_.split(" "))

    val tupleDstream: DStream[(String, Int)] = wordStream.map(w => (w, 1))

    // 无状态
    //val result: DStream[(String, Int)] = tupleDstream.reduceByKey(_ + _)

    // 将RDD转换为有状态，必须设置检查点
    context.sparkContext.setCheckpointDir("data/spark/streaming")

    // buffer 是sparkStreaming自动放入检查点的数据，操作后再放入检查点
    // datas是wordStream中元组的_2的数组
    val re: DStream[(String, Int)] = tupleDstream.updateStateByKey(
      (datas: Seq[Int], buffer: Option[Int]) => {
        // 原先有结果取出
        var result: Int = buffer.getOrElse(0)
        var sum = result + datas.sum
        // 返回新的buffer
        Option(sum)
      }
    )

    re.print
    // 开启接收器
    context.start()

    // main的是driver，需要一直启动，等待接收器执行
    context.awaitTermination()
  }
}