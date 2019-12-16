package com.stt.flink

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment} // 隐式转换需要

// 流处理wordCount程序 含有入参
object Ch_03_streamWordCount {
  def main(args: Array[String]): Unit = {

    val tool: ParameterTool = ParameterTool.fromArgs(args)
    // 从参数--host 和 --port 中获取
    val (host,port) = (tool.get("host") , tool.getInt("port"))

    // 创建流式处理的执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 不进行任务链优化
//    env.disableOperatorChaining()

    // 接收一个socket文本流
    val socketDataStream: DataStream[String] = env.socketTextStream(host,port)

    // 对每条数据进行处理
    val wordCountDataStream: DataStream[(String, Int)] = socketDataStream
      .flatMap(_.split("\\s+"))
      .filter(_.nonEmpty).startNewChain() // 在filter阶段进行新的任务链优化
      .map((_, 1))
      .keyBy(0) // DataSet 有groupBy操作，在DataStream中有keyBy操作相似的效果
      .sum(1)

    wordCountDataStream.print()

    // 启动executor
    env.execute()
  }
}
