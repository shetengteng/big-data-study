package com.atguigu.temp.upperbounds


object covarianceDemo extends App {
  val t: Temp[Super] = new Temp[Sub]("hello world1")

  println("xx" + t.toString)

  val t2: Temp2[Sub] = new Temp2[Super]("hello world2")
  println("xx" + t.toString)
  //错误
//  val t3: Temp3[Sub] = new Temp3[Super]("hello world3")
//  val t4: Temp3[Super] = new Temp3[Sub]("hello world3")
  //正确.
  val t5: Temp3[Super] = new Temp3[Super]("hello world3")
  val t6: Temp3[Sub] = new Temp3[Sub]("hello world3")

  println("xx" + t.toString)

}

class Temp[+A](title: String) {
  override def toString: String = {
    title
  }
}
class Temp2[-A](title: String) {
  override def toString: String = {
    title
  }
}
class Temp3[A](title: String) {
  override def toString: String = {
    title
  }
}
//支持协变
class Super
class Sub extends Super
