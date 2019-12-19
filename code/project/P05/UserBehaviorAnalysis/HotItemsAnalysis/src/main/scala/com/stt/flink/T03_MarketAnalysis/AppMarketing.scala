package com.stt.flink.T03_MarketAnalysis

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


//- 基本需求
//– 从埋点日志中，统计 APP 市场推广的数据指标
//– 按照不同的推广渠道，分别统计数据
//- 解决思路
//– 通过过滤日志中的用户行为，按照不同的渠道进行统计
//– 可以用 process function 处理，得到自定义的输出数据信息
object AppMarketing {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream: DataStream[MarketingUserBehavior] = env.addSource(new SimulatedEventSource())
      .assignAscendingTimestamps(_.timestamp)

    dataStream
      .filter(_.behavior != "UNINSTALL")
      .map(data => ("dummyKey", 1L))
      .keyBy(_._1)
      .timeWindow(Time.hours(1), Time.seconds(5))
      .aggregate(new CountProcess(), new MarketingProcess()) // 预聚合，增量聚合
      .print("re")

    env.execute("AppMarketing")

  }

  class CountProcess() extends AggregateFunction[(String, Long), Long, Long] {
    override def add(value: (String, Long), accumulator: Long): Long = accumulator + 1

    override def createAccumulator(): Long = 0L

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
  }

  //<IN, OUT, KEY, W extends Window>
  class MarketingProcess() extends WindowFunction[Long, MarketingViewCount, String, TimeWindow] {
    override def apply(key: String,
                       window: TimeWindow,
                       input: Iterable[Long],
                       out: Collector[MarketingViewCount]): Unit = {
      out.collect(
        MarketingViewCount(
          window.getStart.toString,
          window.getEnd.toString,
          "app",
          "total",
          input.sum
        )
      )

    }
  }


}