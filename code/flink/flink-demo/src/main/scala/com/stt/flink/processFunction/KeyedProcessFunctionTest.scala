package com.stt.flink.processFunction

import com.stt.flink.source.SensorEntity
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector


object  KeyedProcessFunctionTest {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val dataStream: DataStream[String] = env.socketTextStream("hadoop102", 8888)

    val sensorStream: DataStream[SensorEntity] = dataStream
      .map(item => {
        val fields: Array[String] = item.split(",")
        SensorEntity(fields(0), fields(1).trim.toLong, fields(2).trim.toDouble)
      })

    val warning: DataStream[String] =
      sensorStream.keyBy(_.id).process(new TemperaIncreaseAlertFunction)

    warning.print("warning")

    sensorStream.print("input Data")

    env.execute("KeyedProcessFunctionTest")
  }

  /**
    * 自定义处理函数，用于监控传感器的温度，如果在10s内连续上升，则报警
    */
  class TemperaIncreaseAlertFunction extends KeyedProcessFunction[String,SensorEntity,String]{

    // 使用状态变量
    lazy val lastTemp: ValueState[Double] = getRuntimeContext.getState(
      new ValueStateDescriptor[Double]("lastTemp",classOf[Double])
    )

    // 记录定时器，定时器使用Long表示
    lazy val currentTimer: ValueState[Long] = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("timer",classOf[Long])
    )

    override def processElement(value: SensorEntity,
                                ctx: KeyedProcessFunction[String, SensorEntity, String]#Context,
                                out: Collector[String]): Unit = {
      // 获取上一个温度
      val prevTemp = lastTemp.value()
      // 更新当前温度
      lastTemp.update(value.temperature)

      // 当前温度大于上一个温度，且定时器没有开启
      if(value.temperature > prevTemp){
        if(currentTimer.value() == 0L){
          // 开启一个10s的定时器，这里依据需要，可创建EventTimeTimer
          val timestamp: Long = ctx.timerService().currentProcessingTime()+10*1000
          ctx.timerService().registerProcessingTimeTimer(timestamp)
          currentTimer.update(timestamp)
        }
      }
      if(prevTemp == 0.0 || value.temperature <= prevTemp){
        // 如果温度下降，则取消定时器
        ctx.timerService().deleteProcessingTimeTimer(currentTimer.value())
        currentTimer.clear()
      }

    }

    override def onTimer(timestamp: Long,
                         ctx: KeyedProcessFunction[String, SensorEntity, String]#OnTimerContext,
                         out: Collector[String]): Unit = {

      // 触发了定时器，说明在10s有连续的温度上升
      out.collect(ctx.getCurrentKey+" 温度10s内连续上升")
      // 注意定时器触发完成后，需要清空，下次使用
      currentTimer.clear()
    }
  }
}

