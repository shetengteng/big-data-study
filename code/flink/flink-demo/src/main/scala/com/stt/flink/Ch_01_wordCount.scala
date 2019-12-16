package com.stt.flink

import org.apache.flink.api.scala.{AggregateDataSet, DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment // 隐式转换需要

// 批处理wordCount程序
object Ch_01_wordCount {
  def main(args: Array[String]): Unit = {
    // 创建运行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    // 从文件中读取数据
    val inputPath = this.getClass.getClassLoader.getResource("01_wordCount.txt").getPath

    val inputDS: DataSet[String] = env.readTextFile(inputPath)

    // 分词之后，对单词进行groupby 分组，然后用sum进行聚合
    val wordCountDS: DataSet[(String, Int)] = inputDS
      .flatMap(_.split("\\s+")) // 对每一行进行空格分隔
      .map((_, 1)) // 转换为元组
      .groupBy(0) // 对第一个元素进行分组
      .sum(1)

    // 打印输出
    wordCountDS.print()
    // 输出到本地
    wordCountDS.writeAsCsv("ss.csv",writeMode = WriteMode.OVERWRITE).setParallelism(1)

    env.execute("ch01_wordCount")

  }
}
