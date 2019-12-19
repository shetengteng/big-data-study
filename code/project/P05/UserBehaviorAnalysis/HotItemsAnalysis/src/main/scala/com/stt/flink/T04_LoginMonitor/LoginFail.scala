package com.stt.flink.T04_LoginMonitor

import java.util

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment,_}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer


// 输入样例类
case class LoginEvent(
                     userId: Long,
                     ip: String,
                     eventType: String,
                     timestamp: Long
                   )

// 输出数据格式
case class Warning(
                    userId: Long,
                    firstFailTime: Long,
                    lastFailTime: Long,
                    warningMsg: String
                  )

object LoginFail {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream: DataStream[LoginEvent] = env
      .readTextFile(this.getClass.getClassLoader.getResource("LoginLog.csv").getPath)
      .map(data => {
        val fields = data.split(",")
        LoginEvent(fields(0).trim.toLong, fields(1).trim, fields(2).trim, fields(3).trim.toLong)
      })
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(5)) {
          override def extractTimestamp(element: LoginEvent): Long = {
            element.timestamp * 1000L
          }
        }
      )

    val keyedStream: KeyedStream[LoginEvent, Long] = dataStream.keyBy(_.userId)

    val failDataStream: DataStream[Warning] =
      keyedStream.process(new UserLoginFailProcess(2))

    failDataStream.print("re")

    env.execute("LoginFail")

  }

  // 超过times登录失败，算作攻击
  class UserLoginFailProcess(times: Int) extends KeyedProcessFunction[Long,LoginEvent,Warning]{

    lazy val loginList : ListState[LoginEvent] = getRuntimeContext.getListState(
      new ListStateDescriptor[LoginEvent]("log-event",classOf[LoginEvent])
    )

    lazy val hasFail: ValueState[Boolean] = getRuntimeContext.getState(
      new ValueStateDescriptor[Boolean]("fail-state",classOf[Boolean])
    )

    override def processElement(login: LoginEvent,
                                ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#Context,
                                out: Collector[Warning]): Unit = {
      if(login.eventType == "fail"){
        loginList.add(login)
        // 登录失败 且第一次失败
        if(hasFail.value() == false){
          hasFail.update(true)
          ctx.timerService().registerEventTimeTimer(login.timestamp*1000+2*1000)
        }
      }else{
        // 有一次成功，就取消失败状态，如果2s内有大量的失败，然后有一次成功则定时器获取不到正确的报警
        hasFail.update(false)
        loginList.clear()
      }
    }

    override def onTimer(timestamp: Long,
                         ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#OnTimerContext,
                         out: Collector[Warning]): Unit = {

      val listBuffer = new ListBuffer[LoginEvent]

      val iter: util.Iterator[LoginEvent] = loginList.get().iterator()
      while(iter.hasNext){
        listBuffer += iter.next()
      }

      if (listBuffer.size >= times){
        out.collect(Warning(
          listBuffer.head.userId,
          listBuffer.head.timestamp,
          listBuffer.last.timestamp,
          "登录失败次数超过"+times
        ))

        loginList.clear()
        hasFail.update(false)
      }
    }
  }

}
