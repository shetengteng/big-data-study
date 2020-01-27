package com.stt.flink.T05_OrderMonitor

import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

import scala.collection.Map


// 输入数据
case class OrderEvent(
                       orderId: Long,
                       orderType: String,
                       timestamp: Long
                     )

// 输出数据
case class OrderResult(
                        orderId: Long,
                        eventType: String
                      )

/**
  * 订单超过15分钟没有下单则输出信息
  */
object OrderTimeoutByCEP {

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
    val keyedStream: KeyedStream[OrderEvent, Long] = dataStream.keyBy(_.orderId)

    val pattern: Pattern[OrderEvent, OrderEvent] =
      Pattern.begin[OrderEvent]("start-stage").where(_.orderType.equals("create"))
      .followedBy("followed-stage").where(_.orderType.equals("pay"))
      .within(Time.minutes(15))

    // 定义一个侧输出流标签，输出超时的定点信息
    val orderOutputTag = new OutputTag[OrderResult]("order-waring")

    val result: DataStream[OrderResult] = CEP.pattern(keyedStream, pattern)
      .select(orderOutputTag){
        //  对于已超时的部分模式匹配的事件序列，会调用这个函数
        (pattern: Map[String,Iterable[OrderEvent]],timestamp: Long) => {
          val orderId = pattern.get("start-stage").get.iterator.next().orderId
          OrderResult(orderId,"timeout")
        }
      }{
        //  检测到定义好的模式序列时，就会调用这个函数，按时的订单
        pattern: Map[String,Iterable[OrderEvent]] =>{
          val orderId = pattern.get("followed-stage").get.iterator.next().orderId
          OrderResult(orderId,"success")
        }
      }

    result.getSideOutput(orderOutputTag).print("timeout")
//    result.print("success")
//
    env.execute("OrderTimeoutByCEP")
  }
}