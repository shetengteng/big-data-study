package com.stt.flink.T05_OrderMonitor

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
  * 订单超过15分钟没有下单则输出信息
  */
object OrderTimeoutByProcessFunc2 {

  val outputTag = new OutputTag[OrderResult]("timeout")

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream: DataStream[OrderEvent] = env
      .readTextFile(this.getClass.getClassLoader.getResource("OrderLog.csv").getPath)
      .map(data => {
        val fields: Array[String] = data.split(",")
        OrderEvent(fields(0).trim.toLong, fields(1).trim, fields(3).trim.toLong)
      })
//      .assignAscendingTimestamps(_.timestamp * 1000)
      .assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[OrderEvent](Time.milliseconds(100)) {
        override def extractTimestamp(element: OrderEvent): Long = {
          element.timestamp*1000
        }
      }
    )

    // 通过订单号进行分流
    val keyedStream: KeyedStream[OrderEvent, Long] =
      dataStream.keyBy(_.orderId)

    val result: DataStream[OrderResult] = keyedStream.process(new OrderProcess())

    result.getSideOutput(outputTag).print("timout:")
    result.print("re:")

    env.execute("OrderTimeoutByProcessFunc2")
  }

  /**
    * 使用一个定时器，如果15分钟没有pay，那么放入到侧输出流中
    */
  class OrderProcess() extends KeyedProcessFunction[Long,OrderEvent,OrderResult]{

    // 判断是否支付过
    lazy val payed : ValueState[Boolean] = getRuntimeContext.getState(
      new ValueStateDescriptor[Boolean]("payed",classOf[Boolean])
    )

    lazy val timer: ValueState[Long] = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("timer",classOf[Long])
    )

    override def processElement(value: OrderEvent,
                                ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context,
                                out: Collector[OrderResult]): Unit = {

      if(value.orderType.equals("create")){
        if(payed.value() == true){
          payed.clear()
          timer.clear()
          return
        }
        var ts = value.timestamp * 1000 + 15*60*1000
        payed.update(false)
        timer.update(ts)
      }

      if(value.orderType.equals("pay")){

       if(timer.value() == null || value.timestamp*1000 < timer.value()){
         out.collect(OrderResult(value.orderId,"success"))
         payed.update(true)
       }else{
         ctx.output(outputTag,OrderResult(ctx.getCurrentKey,"timeout"))
         payed.clear()
       }
       timer.clear()
      }
    }
  }
}