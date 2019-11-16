package com.atguigu.temp.generic

object Generic03 {
  def main(args: Array[String]): Unit = {
    val list1 = List(1, 2, 3)
    val list2 = List("abc", "hello", "ok")
    println(getMidEle[Int](list1))
    println(getMidEle[String](list2))
  }

  def getMidEle[T](l: List[T]): T = {
    l(l.length / 2)
  }
}
