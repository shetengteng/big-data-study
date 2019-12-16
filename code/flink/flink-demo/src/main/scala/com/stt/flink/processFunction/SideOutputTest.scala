package com.stt.flink.processFunction

import com.stt.flink.source.SensorEntity
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object SideOutputTest {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val dataStream: DataStream[String] = env.socketTextStream("hadoop102", 8888)

    val sensorStream: DataStream[SensorEntity] = dataStream
      .map(item => {
        val fields: Array[String] = item.split(",")
        SensorEntity(fields(0), fields(1).trim.toLong, fields(2).trim.toDouble)
      })


    val result: DataStream[SensorEntity] = sensorStream.process(new FreezingMonitor)

    result.print("output Data")
    result.getSideOutput(new OutputTag[SensorEntity]("freezing-alarms")).print("freezing")

    env.execute("SideOutputTest")
  }

  /**
    *  华氏温度低于32度（0度），作为低温，第一个泛型是输入类型，第二个泛型是输出类型
    */
  class FreezingMonitor extends ProcessFunction[SensorEntity,SensorEntity]{

    // 定义一个侧输出流标签，需要指定侧输出流的类型
    lazy val freezingAlarmOutput: OutputTag[SensorEntity] =
      new OutputTag[SensorEntity]("freezing-alarms")

    override def processElement(value: SensorEntity,
                                ctx: ProcessFunction[SensorEntity, SensorEntity]#Context,
                                out: Collector[SensorEntity]): Unit = {
      if(value.temperature < 32.0){
        // 侧输出流
        ctx.output(freezingAlarmOutput,value)
      }else{
        // 主输出流
        out.collect(value)
      }
    }
  }

}