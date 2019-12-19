package com.stt.flink.T02

import com.stt.flink.T01.UserBehavior
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable

case class UVCount(
                    windowEnd: Long,
                    uvCount: Long
                  )
/**
  * 使用set的方式进行过滤
  */
object UniqueVisitor {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream: DataStream[String] = env
      .readTextFile(this.getClass.getClassLoader.getResource("UserBehavior.csv").getPath)

    val uvDStream = dataStream
      .map(data => {
        val fields = data.split(",")
        UserBehavior(
          fields(0).trim.toLong,
          fields(1).trim.toLong,
          fields(2).trim.toInt,
          fields(3).trim,
          fields(4).toLong
        )
      }).assignAscendingTimestamps(_.timestamp * 1000)
      .filter(_.behavior == "pv") // 只统计pv操作
      .timeWindowAll(Time.hours(1))
      .apply(new UVCountProcess())

    uvDStream.print("uv")

    env.execute("UniqueVisitor")
  }

  // 没有进行key操作，同时对所有窗口内元素进行操作
  class UVCountProcess() extends AllWindowFunction[UserBehavior,UVCount,TimeWindow]{
    override def apply(window: TimeWindow,
                       input: Iterable[UserBehavior],
                       out: Collector[UVCount]): Unit = {
      // 定义一个set ,进行去重，保存所有的userId，最后输出set的大小
      val set = mutable.Set[Long]()
      val iter = input.iterator
      while (iter.hasNext){
        set += iter.next().userId
      }
      out.collect(UVCount(window.getEnd,set.size))
    }
  }

}