package com.atguigu.chapter06.method

object MethodDemo01 {
  def main(args: Array[String]): Unit = {
    //使用一把
    val dog = new Dog
    println(dog.cal(10, 20))
  }
}

class Dog {
  private var sal: Double = _
  var food: String = _

  //方法
  def cal(n1: Int, n2: Int): Int = {
    return n1 + n2
  }
}

