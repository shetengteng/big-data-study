package com.atguigu.chapter07.scalapackage.hello2

object TestTiger {
  def main(args: Array[String]): Unit = {
    //使用xh的Tiger
    val tiger1 = new com.atguigu.chapter07.scalapackage.xh.Tiger
    //使用xm的Tiger
    val tiger2 = new com.atguigu.chapter07.scalapackage.xm.Tiger
    println(tiger1 + " " + tiger2)

  }
}

class Employee {

}
