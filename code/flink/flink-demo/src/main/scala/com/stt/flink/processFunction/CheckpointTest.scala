package com.stt.flink.processFunction

import com.stt.flink.source.SensorEntity
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object CheckpointTest {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    // 触发checkpoint的时间间隔
    env.enableCheckpointing(60*1000)
    // 设置状态一致性的级别，默认的是EXACTLY_ONCE
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE)
    // 设置超时时间,checkpoint可能会IO保存超时
    env.getCheckpointConfig.setCheckpointTimeout(100*1000)
    // checkpoint保存异常，默认是true，表示如果checkpoint失败，则整个任务停止
    env.getCheckpointConfig.setFailOnCheckpointingErrors(false)
    // 同时checkpoint并行存在，由于IO导致有些checkpoint没有保存完成，默认值是1
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    // 2次checkpoint操作的时间间隔
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(100)
    // checkpoint的外部持久化，job失败取消后，外部的checkpoint信息会被清除，设置RETAIN_ON_CANCELLATION，则需要手动清理
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    // 配置重启策略,job出现错误后，尝试3次，每次间隔500ms
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,500))
    // 失败率重启,5分钟失败率测量时间范围内，最多重启3次，每次10s的间隔，如果3次都失败，则job判断为失效
    // 与 固定延时重启多了个5分钟内时间范围
    env.setRestartStrategy(RestartStrategies.failureRateRestart(3,Time.minutes(5),Time.seconds(10)))


    val dataStream: DataStream[String] = env.socketTextStream("hadoop102", 8888)

    val sensorStream: DataStream[SensorEntity] = dataStream
      .map(item => {
        val fields: Array[String] = item.split(",")
        SensorEntity(fields(0), fields(1).trim.toLong, fields(2).trim.toDouble)
      })


    val result: DataStream[SensorEntity] = sensorStream.process(new FreezingMonitor)

    result.print("output Data")
    result.getSideOutput(new OutputTag[SensorEntity]("freezing-alarms")).print("freezing")

    env.execute("CheckpointTest")
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