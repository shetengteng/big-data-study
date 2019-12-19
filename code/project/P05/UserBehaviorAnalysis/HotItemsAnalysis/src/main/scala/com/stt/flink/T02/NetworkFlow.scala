package com.stt.flink.T02

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneOffset}

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment, WindowedStream,_}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer


case class ApacheLogEvent(
                         ip:String,
                         userId:String,
                         eventTime:Long,
                         method: String,
                         url: String
                         )

// 窗口聚合样例类
case class UrlViewCount(
                       url:String,
                       windowEnd: Long,
                       count: Long
                       )

/**
  * - 基本需求
    - 从web服务器的日志中，统计实时的热门访问页面
    - 统计每分钟的ip访问量，取出访问量最大的5个地址，每5秒更新一次
  - 解决思路
    - 将 apache 服务器日志中的时间，转换为时间戳，作为 Event Time
    - 构建滑动窗口，窗口长度为1分钟，滑动距离为5秒
  */

object NetworkFlow {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream: DataStream[String] = env
      .readTextFile(this.getClass.getClassLoader.getResource("apache.log").getPath)

    // 提取数据，转换数据
    val eventLogDataStream: DataStream[ApacheLogEvent] = dataStream.map(
      data => {
        val fields: Array[String] = data.split("\\s+")

        val timestamp = LocalDateTime
          .parse(fields(3), DateTimeFormatter.ofPattern("dd/MM/yyyy:HH:mm:ss"))
          .toInstant(ZoneOffset.ofHours(-8))
          .toEpochMilli

        ApacheLogEvent(fields(0).trim, fields(1).trim, timestamp, fields(5).trim, fields(6).trim)
      }
    ).assignTimestampsAndWatermarks( // 由于日志的时间是乱序的，需要使用watermark，需要按照经验设置延时大小
      new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1)) {
        override def extractTimestamp(element: ApacheLogEvent): Long = {
          element.eventTime
        }
      }
    )

    // 开辟窗口
    val eventWindowStream: WindowedStream[ApacheLogEvent, String, TimeWindow] = eventLogDataStream
        .filter( data =>{ // 对url不满足条件的进行过滤
          val pattern = "^((?!\\.(css|js)$).)*$".r
          (pattern findFirstIn(data.url)).nonEmpty
        })
        .keyBy(_.url)
        .timeWindow(Time.seconds(10), Time.seconds(5))
        .allowedLateness(Time.seconds(60))     // 允许获取迟到60s的数据

    // 进行聚合操作
    val aggDataStream: DataStream[UrlViewCount] = eventWindowStream
      .aggregate(new CountAggForEventLog(),new WindowResult())

    // 进行分组操作排序
    val value: KeyedStream[UrlViewCount, Long] = aggDataStream
      .keyBy(_.windowEnd)

    value.process(new TopNProcess(3)).print("result")

    env.execute("NetworkFlow")
  }

  class CountAggForEventLog() extends AggregateFunction[ApacheLogEvent,Long,Long]{
    override def add(value: ApacheLogEvent, accumulator: Long): Long = accumulator + 1
    override def createAccumulator(): Long = 0L
    override def getResult(accumulator: Long): Long = accumulator
    override def merge(a: Long, b: Long): Long = a + b
  }

// IN, OUT, KEY, W <: Window
  class WindowResult() extends WindowFunction[Long,UrlViewCount,String,TimeWindow]{
    override def apply(key: String, window: TimeWindow, input: Iterable[Long],
                       out: Collector[UrlViewCount]): Unit = {
      out.collect(UrlViewCount(key,window.getEnd,input.sum))
    }
  }

  // K I O key是windowEnd 是Long类型
  class TopNProcess(n: Int) extends KeyedProcessFunction[Long,UrlViewCount,String]{

    lazy val listState : ListState[UrlViewCount] =  getRuntimeContext.getListState(
      new ListStateDescriptor[UrlViewCount]("list-state",classOf[UrlViewCount])
    )

    override def processElement(value: UrlViewCount,
                                ctx: KeyedProcessFunction[Long, UrlViewCount, String]#Context,
                                out: Collector[String]): Unit = {

      listState.add(value)
      ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
    }

    override def onTimer(timestamp: Long,
                         ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext,
                         out: Collector[String]): Unit = {

      var buffer:ListBuffer[UrlViewCount] = new ListBuffer()
      var result = new StringBuilder("时间：").append(timestamp - 1).append("\n")

      val iter = listState.get().iterator()
      while(iter.hasNext){
        buffer+=iter.next()
      }

      listState.clear()

      buffer.sortWith(_.count > _.count).take(n).foreach(item => {
        result.append("url:").append(item.url)
          .append(" count:").append(item.count).append("\n")
      })

      Thread.sleep(1000)

      out.collect(result.toString())
    }
  }
}