package com.atguigu.temp.generic

object GenericDemo01 {
  def main(args: Array[String]): Unit = {
    val intMessage = new IntMessage[Int](100)
    println("val" + intMessage)

    val stringMessage = new StringMessage[String]("hello")
    println("val=" + stringMessage)

  }
}

abstract class Message[T](s: T) {
  def get: T = s
}

class IntMessage[Int](mes: Int) extends Message(mes)
class StringMessage[String](mes: String) extends Message(mes)
