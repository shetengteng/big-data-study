package com.stt.flink.source

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

import scala.util.Random


object Ch07_CustomSource {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream: DataStream[SensorEntity] = env.addSource(new MySensorSource)

    stream.print().setParallelism(1)

    env.execute("Ch07_CustomSource")
  }

  class MySensorSource extends SourceFunction[SensorEntity]{

    var running : Boolean = true

    override def cancel(): Unit = {
      running = false
    }

    override def run(sc: SourceFunction.SourceContext[SensorEntity]): Unit = {
      val rand = new Random()

      var curTemp = (1 to 10).map(
        i => SensorEntity("s"+i, 0L,23+rand.nextGaussian()*20) // nextGaussian 得到一个高斯分布的值
      )

      while(running){

        // 业务场景，10个温度检测，每次温度的变化在原先的温度的基础上
        // 更新温度 和 时间
        curTemp.foreach{
            case SensorEntity(id,t,temperature) =>
              sc.collect(
                SensorEntity(
                  id,
                  System.currentTimeMillis(),
                  temperature + rand.nextGaussian()
                )
              )
        }

        Thread.sleep(100)
      }
    }
  }
}