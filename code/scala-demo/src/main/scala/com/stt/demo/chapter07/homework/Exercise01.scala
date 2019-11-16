package com.atguigu.chapter07.homework

object Exercise01 {
  def main(args: Array[String]): Unit = {
    //测试
    val cur = new Time(10, 20)
    val other = new Time(10, 20)
    println(cur.before(other)) //结果根据输入的对象不同而定
  }
}

class Time(hrs: Int, min: Int) { //构造器
  val hours: Int = hrs //属性
val minutes: Int = min //属性
def before(other: Time): Boolean = { //方法
  if (hours < other.hours) { //如果当前的小时数小于other
    true
  } else if (hours > other.hours) { //如果当前的小时数大于other
    false
  } else { //小时数相等，判断分钟
    if (minutes < other.minutes) true else false
  }
}
}
