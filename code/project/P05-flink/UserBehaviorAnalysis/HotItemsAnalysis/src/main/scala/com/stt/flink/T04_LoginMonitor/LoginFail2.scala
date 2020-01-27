package com.stt.flink.T04_LoginMonitor

import java.util

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks


/**
  * 做法1只能隔 2 秒之后去判断一下这期间是否有多次失败登录，而不是在一
  * 次登录失败之后、再一次登录失败时就立刻报警
  * 这个需求如果严格实现起来，相当于要判断任意紧邻的事件，是否符合某种模式
  */
object LoginFail2 {

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

    env.execute("LoginFail2")

  }

  // 2秒内 超过times登录失败，给出报警信息
  class UserLoginFailProcess(times: Int) extends KeyedProcessFunction[Long, LoginEvent, Warning] {

    lazy val loginList: ListState[LoginEvent] = getRuntimeContext.getListState(
      new ListStateDescriptor[LoginEvent]("log-event", classOf[LoginEvent])
    )

    override def processElement(login: LoginEvent,
                                ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#Context,
                                out: Collector[Warning]): Unit = {
      if (login.eventType == "fail") {
        loginList.add(login)
      } else {
        loginList.clear()
        return
      }

      // 判断是否有失败状态
      var listBuffer = new ListBuffer[LoginEvent]
      val iter: util.Iterator[LoginEvent] = loginList.get().iterator()
      while (iter.hasNext) {
        listBuffer += iter.next()
      }
      // 对时间进行排序
      listBuffer = listBuffer.sortWith(_.timestamp < _.timestamp)

      if (listBuffer.size >= times) {

        var index = 0;
        Breaks.breakable {
          for (i <- 0 until listBuffer.size) {
            // 需要判断时间是2s内
            if (listBuffer.last.timestamp - listBuffer(i).timestamp <= 2) {
              index = i
              Breaks.break()
            }
          }
        }
        // 在2s内超过失败次数times
        if (listBuffer.size - index >= times) {
          out.collect(Warning(listBuffer.head.userId, listBuffer.head.timestamp, listBuffer.last.timestamp,
            "登录失败次数超过" + times))
          loginList.clear()
          return
        }

        if (index == 0) {
          return
        }

        loginList.clear()
        for (i <- index until listBuffer.size) {
          loginList.add(listBuffer(i))
        }
      }
    }
  }

}
