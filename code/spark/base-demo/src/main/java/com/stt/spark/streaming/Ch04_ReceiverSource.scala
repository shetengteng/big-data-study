package com.stt.spark.streaming

import java.io.{BufferedReader, IOException, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.control.Breaks

object Ch04_ReceiverSource {
  def main(args: Array[String]): Unit = {

    var conf = new SparkConf().setMaster("local[*]").setAppName("Ch04_ReceiverSource")
    // 5s读取一次
    val context: StreamingContext = new StreamingContext(conf,Seconds(5))

    val dStream: ReceiverInputDStream[String] = context.receiverStream(new SocketReceiver("hadoop102",9999))

    dStream.flatMap(_.split(" ")).map(w=>(w,1)).reduceByKey(_ + _).print

    // 开启接收器
    context.start()

    // main的是driver，需要一直启动，等待接收器执行
    context.awaitTermination()
  }
}

class SocketReceiver(host:String,port:Int) extends Receiver[String](StorageLevel.MEMORY_ONLY){

  def receive(): Unit = {
    var runFlag = true
    // 如果服务器没有开启，需要等待
    try{
        val socket = new Socket(host,port)
        val reader = new BufferedReader(new InputStreamReader(socket.getInputStream(),StandardCharsets.UTF_8))
        var line = ""
        // 当receiver没有关闭并且输入数据不为空，则循环发送数据给Spark
        // 网络流当中 socket是没有结尾的，readLine是不会获取null的
        // 一般的处理方式为数据的传递和数据的接收要统一规范
        // 类似自定义协议，可以使用--END表示流结束
        Breaks.breakable{
          while((line=reader.readLine())!=null && !isStopped()){
            if("--END".equals(line)){
              Breaks.break()
            }
            store(line)
          }
        }
        reader.close()
        // 添加该socket.close可能会出错，等同于装饰者模式，socket已经关了
        // socket.close()

//        restart("restart")
     }catch {
       case e: IOException => {
         runFlag = false
       }
     }

    if(runFlag == false){
      Thread.sleep(1000)
      return
    }
  }

  override def onStart(): Unit = {
    new Thread(){
      override def run(): Unit = {
        while(true){
          receive()
        }
      }
    }.start()
  }

  override def onStop(): Unit = {

  }
}
