package com.stt.flink.T03_MarketAnalysis

import java.util.UUID
import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment,_}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

// 输入数据
case class MarketingUserBehavior(
                                  userId: String,
                                  behavior: String,
                                  channel: String,
                                  timestamp: Long
                                )

// 自定义数据源
class SimulatedEventSource() extends RichSourceFunction[MarketingUserBehavior]{

  var flag = true

  val behaviorTypes = Seq("CLICK","DOWNLOAD","INSTALL","UNINSTALL")
  val channelTypes = Seq("wechat","weibo","appstore","huaweistore")
  val rand = new Random()

  override def cancel() = flag = false

  override def run(ctx: SourceFunction.SourceContext[MarketingUserBehavior]) = {
    // 定义一个生成数据的上限
    val maxElement = Long.MaxValue
    var count = 0L

    // 随机生成有效数据
    while(flag && count < maxElement){
      val uuid = UUID.randomUUID().toString
      val behavior = behaviorTypes(rand.nextInt(behaviorTypes.size))
      val channel = channelTypes(rand.nextInt(channelTypes.size))
      val ts = System.currentTimeMillis()

      ctx.collect(MarketingUserBehavior(uuid,behavior,channel,ts))

      count += 1

      TimeUnit.MILLISECONDS.sleep(10)
    }
  }
}

// 输出数据
case class MarketingViewCount(
                               windowStart: String,
                               windowEnd: String,
                               channel: String,
                               behavior: String,
                               count: Long
                             )

//- 基本需求
//– 从埋点日志中，统计 APP 市场推广的数据指标
//– 按照不同的推广渠道，分别统计数据
//- 解决思路
//– 通过过滤日志中的用户行为，按照不同的渠道进行统计
//– 可以用 process function 处理，得到自定义的输出数据信息
object AppMarketingByChannel {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream: DataStream[MarketingUserBehavior] = env.addSource(new SimulatedEventSource())
      .assignAscendingTimestamps(_.timestamp)

    dataStream
      .filter(_.behavior != "UNINSTALL")
      .map(data => ((data.behavior,data.channel),1L))
      .keyBy(_._1)
      .timeWindow(Time.hours(1),Time.seconds(5))
      .process(new MarketingProcess()) // 全量统计
      .print("re")

    env.execute("AppMarketingByChannel")

  }

  // [IN, OUT, KEY, W <: Window]
  class MarketingProcess() extends ProcessWindowFunction[((String,String),Long),MarketingViewCount,(String,String),TimeWindow]{

    override def process(key: (String, String),
                         context: Context,
                         elements: Iterable[((String, String), Long)],
                         out: Collector[MarketingViewCount]): Unit = {
        out.collect(
          MarketingViewCount(
            context.window.getStart.toString,
            context.window.getEnd.toString,
            key._2,
            key._1,
            elements.size
          )
        )
    }
  }

}