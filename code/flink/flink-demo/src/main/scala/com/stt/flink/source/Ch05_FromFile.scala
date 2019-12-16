package com.stt.flink.source

import org.apache.flink.streaming.api.scala._

/**
  * 从文件中读取数据
  */
object Ch05_FromFile {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 从自定义的集合中读取数据
    val sensorStream: DataStream[SensorEntity] = env
      .readTextFile(this.getClass.getClassLoader.getResource("05_sensor.txt").getPath)
      .map(item =>{
        val fields = item.split(",")
        SensorEntity(fields(0).trim,fields(1).trim.toLong,fields(2).trim.toDouble)
      })

    sensorStream.print("test02").setParallelism(1)

    env.execute("Ch05_FromFile")
  }
}
