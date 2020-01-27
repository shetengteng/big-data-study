package com.stt.flink.T02

import com.stt.flink.T01.UserBehavior
import com.stt.flink.T02.UVWithBloom.Bloom
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

/**
  * 使用布隆过滤器的方式进行过滤
  * 将位图存储在redis中
  * 有问题，在窗口结束判断watermark失败
  */
object UVWithBloom2 {

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
      }).assignAscendingTimestamps(_.timestamp * 1000).setParallelism(2)
      .filter(_.behavior == "pv") // 只统计pv操作
      .map(data => ("dummyKey", data.userId)).setParallelism(1)

    uvDStream.keyBy(_._1)
      .timeWindow(Time.hours(1))
      .trigger(new MyTrigger()) // 触发窗口操作
      .process(new UVCountWithBloom()).print("uv")

    env.execute("UVWithBloom2")
  }

  class MyTrigger() extends Trigger[(String, Long), TimeWindow] {
    // 窗口的EventTime定时器触发时调用onEventTime
    override def onEventTime(time: Long,
                             window: TimeWindow,
                             ctx: Trigger.TriggerContext): TriggerResult = {
      TriggerResult.CONTINUE
    }

    override def onProcessingTime(time: Long,
                                  window: TimeWindow,
                                  ctx: Trigger.TriggerContext): TriggerResult = {
      TriggerResult.CONTINUE
    }

    override def clear(window: TimeWindow,
                       ctx: Trigger.TriggerContext): Unit = {}

    // 每个数据到达窗口后触发该方法
    override def onElement(element: (String, Long),
                           timestamp: Long,
                           window: TimeWindow,
                           ctx: Trigger.TriggerContext): TriggerResult = {
      // 直接触发窗口进行计算，如果没有PURGE，那么一直是第一个元素，原因未知
      TriggerResult.FIRE_AND_PURGE
    }
  }

  //[IN, OUT, KEY, W <: Window]
  class UVCountWithBloom() extends ProcessWindowFunction[(String, Long), UVCount, String, TimeWindow] {


    // 定义一个缓存状态，缓存userid
    var userIdState: MapState[Long, Int] = _

    override def open(parameters: Configuration): Unit = {
      userIdState = getRuntimeContext.getMapState(
        new MapStateDescriptor[Long, Int]("userid-map", classOf[Long], classOf[Int])
      )
    }

    lazy val jedis = new Jedis("hadoop102", 6379)
    lazy val bloom = new Bloom(1 << 29) // 64M 5亿的数据


    override def process(key: String,
                         context: Context,
                         elements: Iterable[(String, Long)],
                         out: Collector[UVCount]): Unit = {
      // 将元素放入userIdState中
      userIdState.put(elements.head._2, 1)

      val storeKey = context.window.getEnd.toString

      val userIdList = new ListBuffer[Long]()

      val iter = userIdState.keys().iterator()
      while (iter.hasNext) {
        userIdList += iter.next()
      }

      // 1511661599999
      // 1511661600000
      // 1511658000000

      if (context.window.getEnd <= context.currentWatermark) {
        // 说明窗口结束 有问题，currentWatermark 值始终是最小值
        handle(userIdList, storeKey, out)
        userIdState.clear()
        // 窗口结束打印
        out.collect(UVCount(storeKey.toLong, jedis.hget("count", storeKey).toLong))

      } else {
        // 窗口进行中
        // 满10个元素，并判断是否存在于位图中
        if (userIdList.size >= 1000) {
          // 开始处理
          handle(userIdList, storeKey, out)
          userIdState.clear()
        }
      }
    }

    def handle(userIdList: ListBuffer[Long], storeKey: String, out: Collector[UVCount]) = {
      for (userId <- userIdList) {
        // 位图的存储方式
        // 每个窗口一个位图，key是windowEnd,value是bitmap
        // 用布隆过滤器判断是否存在
        val offset = bloom.hash(userId + "", 67)

        // 从位图中查找是否存在
        if (!jedis.getbit(storeKey, offset)) {
          // 位图的位置设置为1
          jedis.setbit(storeKey, offset, true)
          // 将每个窗口的结果存储到redis中,要先从redis中hash读取
          // count + 1
          jedis.hincrBy("count", storeKey, 1)
        }
        //out.collect(UVCount(storeKey.toLong, jedis.hget("count", storeKey).toLong))
      }
    }
  }

}