package com.stt.flink.T05_OrderMonitor

import com.stt.flink.T05_OrderMonitor.OrderTxMatch.TxCoProcess
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector


// 实时对账,通过网络端口得到数据
object OrderTxMatch2 {

  // 定义侧输出流
  // 在订单order中没有匹配到pay信息
  val unmatchedPays = new OutputTag[OrderTxEvent]("unmatched pays")
  // 在receipt中没有匹配到order信息
  val unmatchedReceipts = new OutputTag[ReceiptEvent]("unmatched receipt")

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 读取Order流
    val orderKeyedStream: KeyedStream[OrderTxEvent, String] =
      env.socketTextStream("hadoop102",7777) // 从7777端口访问
        .map(
          data => {
            val fields: Array[String] = data.split(",")
            OrderTxEvent(fields(0).trim.toLong, fields(1).trim, fields(2).trim, fields(3).trim.toLong)
          }
        ).assignAscendingTimestamps(_.timestamp * 100)
        .filter(_.txId != "")
        .keyBy(_.txId)

    // 读取receipt流
    val receiptKeyedStream: KeyedStream[ReceiptEvent, String] =
      env.socketTextStream("hadoop102",8888) //从8888端口访问
        .map(
          data => {
            val fields = data.split(",")
            ReceiptEvent(fields(0).trim, fields(1).trim, fields(2).trim.toLong)
          }
        ).assignAscendingTimestamps(_.timestamp * 1000)
        .keyBy(_.txId)

    // 将2条流连接
    val connectedStream: ConnectedStreams[OrderTxEvent, ReceiptEvent] =
      orderKeyedStream.connect(receiptKeyedStream)

    val result: DataStream[(OrderTxEvent, ReceiptEvent)] = connectedStream.process(new TxCoProcess(5))

    result.print()
    result.getSideOutput(unmatchedPays).print("unmatchedPays")
    result.getSideOutput(unmatchedReceipts).print("unmatchedReceipts")

    env.execute("OrderTxMatch")
  }

}