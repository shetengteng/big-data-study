package com.stt.flink.T02

import java.lang

import com.stt.flink.T01.UserBehavior
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

/**
  * 使用布隆过滤器的方式进行过滤
  * 将位图存储在redis中
  */
object UVWithBloom {

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
      .map(data => ("dummyKey",data.userId))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .trigger(new MyTrigger()) // 触发窗口操作
      .process(new UVCountWithBloom())

    uvDStream.print("uv")

    env.execute("UVWithBloom")
  }

  class MyTrigger() extends Trigger[(String,Long),TimeWindow]{
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
       // 直接触发窗口进行计算，并清空所有窗口状态
      // 窗口中的状态无法存储
      TriggerResult.FIRE_AND_PURGE
    }
  }

  // 定义一个bloom过滤器
  class Bloom(size: Long) extends Serializable{
    // 位图的总大小 2^27 = 134217728 一亿三千万
    // 2^4*2^3*2^10*2^10 = 16MB
    private val cap = if ( size > 0 ) size else 1 << 27  // 默认16M

    // 定义hash函数,seed一般是质数
    def hash(value:String, seed:Int):Long = {
      var result : Long = 0L
      // 使用字符的累加值得到hash
      for(i<- 0 until value.length){
        result = result * seed + value.charAt(i)
      }
      // 获取 rresult 后 27 位值
      result & (cap -1)
    }
  }

  //[IN, OUT, KEY, W <: Window]
  class UVCountWithBloom() extends ProcessWindowFunction[(String,Long),UVCount,String,TimeWindow]{

    lazy val jedis = new Jedis("hadoop102",6379)
    lazy val bloom = new Bloom(1 << 29) // 64M 5亿的数据

    override def process(key: String,
                         context: Context,
                         elements: Iterable[(String, Long)],
                         out: Collector[UVCount]): Unit = {
      // 位图的存储方式
      // 每个窗口一个位图，key是windowEnd,value是bitmap
      val storeKey = context.window.getEnd.toString

      // 用布隆过滤器判断是否存在
      val userId = elements.head._2.toString
      val offset = bloom.hash(userId,67)

      // 从位图中查找是否存在
      val isExist: lang.Boolean = jedis.getbit(storeKey,offset)

      if(!isExist){

        // 位图的位置设置为1
        jedis.setbit(storeKey,offset,true)

        // 将每个窗口的结果存储到redis中,要先从redis中hash读取
        // count + 1
        val count: lang.Long = jedis.hincrBy("count",storeKey,1)

        out.collect(UVCount(storeKey.toLong,count))
      }else{
        out.collect(UVCount(storeKey.toLong,jedis.hget("count",storeKey).toLong))
      }
    }
  }
}