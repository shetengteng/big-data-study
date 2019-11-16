package com.atguigu.chapter07.myextends

object ScalaFieldOverrideDetail02 {
  def main(args: Array[String]): Unit = {
    println("xxx")
    val bbbbb = new BBBBB()
    println(bbbbb.sal) // 0
    val b2: AAAAA = new BBBBB()
    println("b2.sal=" + b2.sal()) // 0
  }
}

class AAAAA {
  def sal(): Int = {
    return 10
  }
}

class BBBBB extends AAAAA {
  override val sal: Int = 0 //底层 public sal
}
