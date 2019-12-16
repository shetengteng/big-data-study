package com.stt.flink.processFunction

import com.stt.flink.source.SensorEntity
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * 状态编程
  */
object StateTest {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val dataStream: DataStream[String] = env.socketTextStream("hadoop102", 8888)

    val sensorStream: DataStream[SensorEntity] = dataStream
      .map(item => {
        val fields: Array[String] = item.split(",")
        SensorEntity(fields(0), fields(1).trim.toLong, fields(2).trim.toDouble)
      })

    // 方式1.使用flatMap 函数类
    val warning1: DataStream[(String, Double, Double)] =
      sensorStream.keyBy(_.id).flatMap(new TemperatureChangeAlert(10.0))
    warning1.print("warning")

    // 方式2.使用keyed 函数类
    val warning2: DataStream[(String, Double, Double)] =
      sensorStream.keyBy(_.id).process(new TemperatureChangeAlert2(10.0))
    warning2.print("warning2")

    // 方式3.使用flatMapWithState
    // 第一个泛型是返回值，第二个参数是状态类型
    val warning3 = sensorStream.keyBy(_.id).flatMapWithState[(String,Double,Double),Double]{
      // 初始情况，第一次接收
      case (input: SensorEntity,None) => (List.empty,Some(input.temperature))
        // 第二次接收
      case (input: SensorEntity,lastTemp :Some[Double]) =>{
        val diff = (input.temperature - lastTemp.get).abs
        if(diff>10){
          (List((input.id,lastTemp.get,input.temperature)),Some(input.temperature))
        }else{
          (List.empty,Some(input.temperature))
        }
      }
    }

    warning3.print("warinng3")

    sensorStream.print("input Data")

    env.execute("StateTest")
  }

  /**
    * 功能：2次温度之间超过一定温度报警
    * 返回值：id，上次温度，本次温度
    * 由于没有key
    * 不使用FlatMapFunction的原因是没有包含上下文，无法记录状态
    *
    */
  class TemperatureChangeAlert(threshold: Double) extends RichFlatMapFunction[SensorEntity,(String,Double,Double)]{

    var lastTemperature:ValueState[Double] = _

    override def open(parameters: Configuration): Unit = {
      lastTemperature = getRuntimeContext.getState(
        new ValueStateDescriptor[Double]("lastTemp",classOf[Double])
      )
    }

    override def flatMap(in: SensorEntity, out: Collector[(String, Double, Double)]): Unit = {

      if((lastTemperature.value()-in.temperature).abs > threshold){
        out.collect((in.id, lastTemperature.value(),in.temperature))
      }
      lastTemperature.update(in.temperature)
    }

  }

  class TemperatureChangeAlert2(threshold: Double) extends KeyedProcessFunction[String,SensorEntity,(String,Double,Double)]{

    // 此处注意lazy关键字，getRuntimeContext需要先初始化
    lazy val lastTemperature = getRuntimeContext.getState(
      new ValueStateDescriptor[Double]("lastTemp",classOf[Double])
    )

    override def processElement(in: SensorEntity,
                                ctx: KeyedProcessFunction[String, SensorEntity, (String, Double, Double)]#Context,
                                out: Collector[(String, Double, Double)]): Unit = {
      if((lastTemperature.value()-in.temperature).abs > threshold){
        out.collect((in.id, lastTemperature.value(),in.temperature))
      }
      lastTemperature.update(in.temperature)

    }
  }

}

