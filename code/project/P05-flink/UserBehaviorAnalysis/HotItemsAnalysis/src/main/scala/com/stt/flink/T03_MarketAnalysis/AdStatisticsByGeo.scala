package com.stt.flink.T03_MarketAnalysis

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.{ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


// 输入样例类
case class AdClientEvent(
                          userId: Long,
                          adId: Long,
                          province: String,
                          city: String,
                          timestamp: Long
                        )
// 输出样例类
case class AdCountByProvince(
                              province: String,
                              count: Long,
                              windowEnd: Long
                            )
object AdStatisticsByGeo {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    var resource = this.getClass.getClassLoader.getResource("AdClickLog.csv")

    val windowedStream: WindowedStream[AdClientEvent, String, TimeWindow] = env.readTextFile(resource.getPath)
      .map(data => {
        val fields = data.split(",")
        AdClientEvent(
          fields(0).trim.toLong,
          fields(1).trim.toLong,
          fields(2).trim,
          fields(3).trim,
          fields(4).trim.toLong
        )
      }).assignAscendingTimestamps(_.timestamp * 1000)
      .keyBy(_.province)
      .timeWindow(Time.hours(1), Time.seconds(5))

    windowedStream.process(new AdClickProcess())
      .print("re1")// 全量统计
    windowedStream.aggregate(new AdCountAgg(),new AdWindowResult())
      .print("re2")// 增量统计

    env.execute("AdStatisticsByGeo")
  }

  class AdClickProcess() extends ProcessWindowFunction[AdClientEvent,AdCountByProvince,String,TimeWindow]{
    override def process(key: String,
                         context: Context,
                         elements: Iterable[AdClientEvent],
                         out: Collector[AdCountByProvince]): Unit = {
      out.collect(AdCountByProvince(
        key,
        elements.size,
        context.window.getEnd
      ))
    }
  }


  class AdCountAgg() extends AggregateFunction[AdClientEvent,Long,Long]{
    override def add(value: AdClientEvent, accumulator: Long): Long = accumulator + 1
    override def createAccumulator(): Long = 0L
    override def getResult(accumulator: Long): Long = accumulator
    override def merge(a: Long, b: Long): Long = a + b
  }

  class AdWindowResult() extends WindowFunction[Long,AdCountByProvince,String,TimeWindow]{
    override def apply(key: String,
                       window: TimeWindow,
                       input: Iterable[Long],
                       out: Collector[AdCountByProvince]): Unit = {
//     if(input.size != 1){
//       println("==========")
//       println(key)
//       input.foreach(println)
//       println("==========")
//     }

      out.collect(AdCountByProvince(
        key,
        input.sum,
        window.getEnd
      ))
    }
  }

}