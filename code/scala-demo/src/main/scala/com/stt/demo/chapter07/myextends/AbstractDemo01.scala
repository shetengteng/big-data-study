package com.atguigu.chapter07.myextends

object AbstractDemo01 {
  def main(args: Array[String]): Unit = {
    println("xxx")
  }
}

//抽象类
abstract class Animal{
  var name : String //抽象的字段
  var age : Int // 抽象的字段
  var color : String = "black" //普通属性
  def cry() //抽象方法,不需要标记 abstract
}

