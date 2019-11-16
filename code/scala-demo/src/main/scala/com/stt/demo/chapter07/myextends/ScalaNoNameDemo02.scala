package com.atguigu.chapter07.myextends

object ScalaNoNameDemo02 {
  def main(args: Array[String]): Unit = {
    val monster = new Monster {
      override def cry(): Unit = {
        println("妖怪嗷嗷叫...:)")
      }
      override var name: String = _
    }
    monster.cry()
  }
}

abstract class Monster {
  var name: String

  def cry()
}

