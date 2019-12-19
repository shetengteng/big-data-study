package com.stt.flink.T05_OrderMonitor

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

// 定义输入订单事件的样例类
case class TOrderEvent(orderId: Long, eventType: String, txId: String, eventTime: Long)
// 定义接收流事件的样例类
case class TReceiptEvent(txId: String, payChannel: String, eventTime: Long)

object TxMatchByJoin {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 读取订单事件流
    val resource = getClass.getResource("/OrderLog.csv")
    //    val orderEventStream = env.readTextFile(resource.getPath)
    val orderEventStream = env.socketTextStream("localhost", 7777)
      .map(data => {
        val dataArray = data.split(",")
        TOrderEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
      })
      .filter(_.txId != "")
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.txId)

    // 读取支付到账事件流
    val receiptResource = getClass.getResource("/ReceiptLog.csv")
    //    val receiptEventStream = env.readTextFile(receiptResource.getPath)
    val receiptEventStream = env.socketTextStream("localhost", 8888)
      .map( data => {
        val dataArray = data.split(",")
        TReceiptEvent( dataArray(0).trim, dataArray(1).trim, dataArray(2).toLong )
      } )
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.txId)

    // join处理
    val processedStream = orderEventStream.intervalJoin( receiptEventStream )
      .between(Time.seconds(-5), Time.seconds(5))
      .process( new TxPayMatchByJoin() )

    processedStream.print()

    env.execute("tx pay match by join job")
  }
}

class TxPayMatchByJoin() extends ProcessJoinFunction[TOrderEvent, TReceiptEvent, (TOrderEvent, TReceiptEvent)]{
  override def processElement(left: TOrderEvent, right: TReceiptEvent,
                              ctx: ProcessJoinFunction[TOrderEvent, TReceiptEvent, (TOrderEvent, TReceiptEvent)]#Context,
                              out: Collector[(TOrderEvent, TReceiptEvent)]): Unit = {
    out.collect((left, right))
  }
}
