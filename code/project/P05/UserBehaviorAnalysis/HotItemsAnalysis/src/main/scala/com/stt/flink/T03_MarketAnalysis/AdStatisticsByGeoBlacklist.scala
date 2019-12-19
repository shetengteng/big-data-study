package com.stt.flink.T03_MarketAnalysis

import java.time._

import com.stt.flink.T03_MarketAnalysis.AdStatisticsByGeo.{AdCountAgg, AdWindowResult}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector


// 黑名单的报警信息
case class BlackListWaring(
                            userId: Long,
                            adId: Long,
                            msg: String
                          )
/**
  * 针对大量刷单，需要过滤
  */
object AdStatisticsByGeoBlacklist {

  // 侧输出流tag，得到当前的黑名单
  val blackListOutputTag = new OutputTag[BlackListWaring]("blackList")

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    var resource = this.getClass.getClassLoader.getResource("AdClickLog.csv")

    val dataStream: DataStream[AdClientEvent] = env.readTextFile(resource.getPath)
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

    val filterDataStream: DataStream[AdClientEvent] = dataStream
      .keyBy(data => (data.userId, data.adId))
      .process(new FilterBlackListUser(100))// 进行过滤

    filterDataStream
      .keyBy(_.province)
      .timeWindow(Time.hours(1),Time.seconds(5))
      .aggregate(new AdCountAgg(),new AdWindowResult())
      .print("re")

    filterDataStream.getSideOutput(blackListOutputTag).print("black")

    env.execute("AdStatisticsByGeoBlacklist")
  }

  /**
    * 记录状态，每天0点清空黑名单
    * @param n 每个用户一天点击该广告超过n说明刷单，每个userid-adId含有一个FilterBlackListUser用于判断
    */
  class FilterBlackListUser(n: Int) extends KeyedProcessFunction[(Long,Long),AdClientEvent,AdClientEvent]{

    // 该userId点击adId的次数
    lazy val countState: ValueState[Long] = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("count-state",classOf[Long])
    )

    lazy val resetTime: ValueState[Long] = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("reset-time-state",classOf[Long])
    )
    // 表示是否登记过黑名单
    lazy val hasOver: ValueState[Boolean] = getRuntimeContext.getState(
      new ValueStateDescriptor[Boolean]("has-over-state",classOf[Boolean])
    )

    override def processElement(value: AdClientEvent,
                                ctx: KeyedProcessFunction[(Long, Long), AdClientEvent, AdClientEvent]#Context,
                                out: Collector[AdClientEvent]): Unit = {

      if(countState.value() == 0){
        // 除以天数求整数，然后+1天
        val ts: Long = LocalDate
          .ofEpochDay(ctx.timerService().currentProcessingTime()/(24*3600*1000)+1)
          .atStartOfDay(ZoneId.systemDefault()).toEpochSecond*1000
        resetTime.update(ts)
        // 获取下一个0点,刷新countState
        ctx.timerService().registerProcessingTimeTimer(ts)
      }

      if(countState.value() > n){
        if(!hasOver.value()){
          // 超过上限加入到黑名单
          ctx.output(
            blackListOutputTag,
            BlackListWaring(
              value.userId,
              value.adId,
              "click over"+n+" times"
            )
          )
          hasOver.update(true)
        }
        return
      }
      // 点击数+1
      countState.update(countState.value() + 1)
      out.collect(value)
    }

    override def onTimer(timestamp: Long,
                         ctx: KeyedProcessFunction[(Long, Long), AdClientEvent, AdClientEvent]#OnTimerContext,
                         out: Collector[AdClientEvent]): Unit ={
      if(resetTime.value().equals(timestamp)){
        hasOver.clear()
        countState.clear()
      }
    }
  }
}