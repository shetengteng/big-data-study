package com.stt.flink

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._ // 隐式转换需要

// 流处理wordCount程序
object Ch_02_streamWordCount {
  def main(args: Array[String]): Unit = {
    // 创建流式处理的执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 接收一个socket文本流
    val socketDataStream: DataStream[String] = env.socketTextStream("hadoop102",8888)

    // 对每条数据进行处理
    val wordCountDataStream: DataStream[(String, Int)] = socketDataStream
      .flatMap(_.split("\\s+"))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0) // DataSet 有groupBy操作，在DataStream中有keyBy操作相似的效果
      .sum(1)

    wordCountDataStream.print()

    // writeAsText有结果产生，writeAsCsv有问题，结果没有显示
    wordCountDataStream.writeAsText("result/ss.text").setParallelism(1)
    wordCountDataStream.writeAsCsv("result/ss.csv").setParallelism(1)

    // 启动executor
    env.execute("Ch_02_streamWordCount")
  }
}
