package com.stt.flink.T05_OrderMonitor

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

// 输入数据
case class OrderTxEvent(
                         orderId: Long,
                         orderType: String,
                         txId: String, // 交易数据
                         timestamp: Long
                       )

case class ReceiptEvent(
                         txId: String,
                         payChannel: String,
                         timestamp: Long
                       )
// 实时对账
object OrderTxMatch {

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
      env.readTextFile(this.getClass.getClassLoader.getResource("OrderLog.csv").getPath).map(
      data => {
        val fields: Array[String] = data.split(",")
        OrderTxEvent(fields(0).trim.toLong, fields(1).trim, fields(2).trim, fields(3).trim.toLong)
      }
    ).assignAscendingTimestamps(_.timestamp * 100)
      .filter(_.txId != "")
      .keyBy(_.txId)

    // 读取receipt流
    val receiptKeyedStream: KeyedStream[ReceiptEvent, String] =
      env.readTextFile(this.getClass.getClassLoader.getResource("ReceiptLog.csv").getPath).map(
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

  // seconds 是定时器等待的时间
  class TxCoProcess(seconds:Int) extends CoProcessFunction[OrderTxEvent,ReceiptEvent,(OrderTxEvent,ReceiptEvent)]{

    lazy val orderState : ValueState[OrderTxEvent] = getRuntimeContext.getState(
      new ValueStateDescriptor[OrderTxEvent] ("order-state",classOf[OrderTxEvent])
    )
    lazy val payState : ValueState[ReceiptEvent] = getRuntimeContext.getState(
      new ValueStateDescriptor[ReceiptEvent] ("pay-state",classOf[ReceiptEvent])
    )
    // 记录定时器，用于去除
    lazy val timer : ValueState[Long] = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("timer",classOf[Long])
    )

    override def processElement1(pay: OrderTxEvent,
                                 ctx: CoProcessFunction[OrderTxEvent, ReceiptEvent, (OrderTxEvent, ReceiptEvent)]#Context,
                                 out: Collector[(OrderTxEvent, ReceiptEvent)]): Unit = {
      // 说明都到了
      if(payState.value() != null){
        out.collect((pay,payState.value()))
        payState.clear()
        if(timer.value() != null){
          ctx.timerService().deleteEventTimeTimer(timer.value())
        }
        return
      }

      // 如果没有到,开启定时器
      orderState.update(pay)
      timer.update(ts(pay.timestamp))
      ctx.timerService().registerEventTimeTimer(ts(pay.timestamp))
    }

    override def processElement2(receipt: ReceiptEvent,
                                 ctx: CoProcessFunction[OrderTxEvent, ReceiptEvent, (OrderTxEvent, ReceiptEvent)]#Context,
                                 out: Collector[(OrderTxEvent, ReceiptEvent)]): Unit = {
      if(orderState.value() != null){
        out.collect((orderState.value(),receipt))
        orderState.clear()
        if(timer.value() != null){
          ctx.timerService().deleteEventTimeTimer(timer.value())
        }
        return
      }
      payState.update(receipt)
      timer.update(ts(receipt.timestamp))
      ctx.timerService().registerEventTimeTimer(ts(receipt.timestamp))
    }

    // 超时则进行侧输出流输出没有到的元素
    override def onTimer(timestamp: Long,
                         ctx: CoProcessFunction[OrderTxEvent, ReceiptEvent, (OrderTxEvent, ReceiptEvent)]#OnTimerContext,
                         out: Collector[(OrderTxEvent, ReceiptEvent)]): Unit = {
      if(payState.value() == null && orderState.value() != null){
        ctx.output(unmatchedPays,orderState.value())
      }
      if(payState.value() != null && orderState.value() == null){
        ctx.output(unmatchedReceipts,payState.value())
      }
      payState.clear()
      orderState.clear()
      timer.clear()
    }

    def ts(timestamp: Long)={
      timestamp*1000+seconds*1000
    }
  }
}