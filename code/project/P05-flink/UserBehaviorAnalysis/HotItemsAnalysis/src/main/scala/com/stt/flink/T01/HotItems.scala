package com.stt.flink.T01

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

// 输入数据样例类
case class UserBehavior(
                         userId: Long,
                         ItemId: Long,
                         categoryId: Int,
                         behavior: String,
                         timestamp: Long
                       )

// 窗口聚合输出样例类
case class ItemViewCount(
                          itemId: Long,
                          windowEnd: Long,
                          count: Long
                        )
/**
  * • 基本需求
    – 统计近1小时内的热门商品，每5分钟更新一次
    – 热门度用浏览次数（“pv”）来衡量
    • 解决思路
    – 在所有用户行为数据中，过滤出浏览（“pv”）行为进行统计
    – 构建滑动窗口，窗口长度为1小时，滑动距离为5分钟
  */
object HotItems {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 读取数据
    val dataStream: DataStream[String] =
      env.readTextFile(this.getClass.getClassLoader.getResource("UserBehavior.csv").getPath)

    //
    val userBehaviorDataStream = dataStream
      .map(data => {
        val fields = data.split(",")
        UserBehavior(
          fields(0).trim.toLong,
          fields(1).trim.toLong,
          fields(2).trim.toInt,
          fields(3).trim,
          fields(4).toLong
        )
      })
      .assignAscendingTimestamps(_.timestamp * 1000) // 由于是递增时间戳，使用递增timestamp即可

    // 对pv进行过滤
    val windowedStream: WindowedStream[UserBehavior, Long, TimeWindow] = userBehaviorDataStream
      .filter(_.behavior == "pv")
      .keyBy(_.ItemId)
      .timeWindow(Time.hours(1), Time.minutes(5)) // 进行开窗操作，1小时，5分钟滑动

    val itemViewCountDataStream: DataStream[ItemViewCount] = windowedStream
      .aggregate(new CountAgg(), new WindowResult())// 窗口聚合

    // itemViewCountDataStream 表示在该滑动窗口下所有的计数值
    val itemViewCountKeyedStream: KeyedStream[ItemViewCount, Long] = itemViewCountDataStream
      .keyBy(_.windowEnd) // 按照窗口分组，分成多条子流

    val result: DataStream[String] =itemViewCountKeyedStream
      .process(new TopNHotItems(3))// 进行排序，此处的process函数基于KeyedStream操作

    result.print()

    env.execute("HotItems")
  }

  // 预聚合，每次来一个数据，进行累加，IN 输入类型 ACC 中间累加变量 OUT 输出类型
  // 按照相同的key，进行调用聚合
  class CountAgg() extends AggregateFunction[UserBehavior, Long, Long]{

    override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

    override def createAccumulator(): Long = 0L

    override def getResult(accumulator: Long): Long = accumulator

    // 2 个累加器处理
    override def merge(a: Long, b: Long): Long = a + b

  }

  // 自定义结果函数[IN, OUT, KEY, W <: Window]，输出结果
  // 注意KEY的类型由keyBy决定，如果是string类型，key使用tuple，使用的是Long类型，key使用Long
  class WindowResult() extends WindowFunction[Long, ItemViewCount, Long, TimeWindow]{
    override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
//      out.collect(ItemViewCount(key,window.getEnd,input.iterator.next()))
      out.collect(ItemViewCount(key,window.getEnd,input.sum))
    }
  }

  // 自定义处理函数 K,I,O
  class TopNHotItems(n: Int) extends KeyedProcessFunction[Long,ItemViewCount,String]{

    var itemState: ListState[ItemViewCount] = _

    override def open(parameters: Configuration): Unit = {
      itemState = getRuntimeContext.getListState(
        new ListStateDescriptor[ItemViewCount]("item-state",classOf[ItemViewCount])
      )
    }

    override def processElement(value: ItemViewCount,
                                ctx: KeyedProcessFunction[Long, ItemViewCount, String]#Context,
                                out: Collector[String]): Unit = {
      // 将每条数据存入状态列表
      itemState.add(value)
      // 注册一个定时器
      // 注意：在该窗口结束后+1ms开始计算统计排序的值，每个值有该窗口结束的时间
      // 相同的定时器，不会触发多次，内部是一个优先级队列，使用timestamp作为比较参数
      // 定时器的触发基于watermark的到来
      ctx.timerService().registerEventTimeTimer(value.windowEnd+1)

    }

    // 定时器触发时，对所有数据排序，并输出结果
    override def onTimer(timestamp: Long,
                         ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext,
                         out: Collector[String]): Unit = {

      println("#######"+timestamp)

      // 将所有state中的数据取出，放到list buffer中
      val allItems : ListBuffer[ItemViewCount] = new ListBuffer()
      // 支持集合的遍历操作
      import scala.collection.JavaConversions._
      for(item <- itemState.get()){
        allItems += item
      }

      // 按照count大小排序
      val TopN: ListBuffer[ItemViewCount] = allItems.sortBy(_.count)(Ordering.Long.reverse).take(n)

      // 清空状态
      itemState.clear()

      // 结果输出
      val result = new StringBuilder()
      result.append("时间：").append(new Timestamp(timestamp-1)).append("\n")
      // 输出每一个商品的信息
      for(i <- TopN.indices){
        val currentItem = TopN(i)
        result.append("No").append(i+1).append(":")
          .append("商品id=").append(currentItem.itemId)
          .append("浏览量=").append(currentItem.count)
          .append("\n")
      }
      result.append("======================")
      // 控制输出频率，测试显示使用
      Thread.sleep(1000)
      out.collect(result.toString())
    }
  }

  // 自定义预聚合函数，计算平均数
  // 使用元组作为累加器类型(Long 表示和，Int表示个数)
  class AverageAgg() extends AggregateFunction[UserBehavior,(Long,Int),Double]{
    override def add(value: UserBehavior, accumulator: (Long, Int)): (Long, Int) = {
      (
        accumulator._1+value.timestamp,
        accumulator._2+1
      )
    }

    override def createAccumulator(): (Long, Int) = (0L,0)

    override def getResult(accumulator: (Long, Int)): Double = {
      if(accumulator._2 == 0){
        0D
      }else{
        accumulator._1 / accumulator._2
      }
    }

    override def merge(a: (Long, Int), b: (Long, Int)): (Long, Int) = (a._1+b._1,a._2+b._2)

  }
}