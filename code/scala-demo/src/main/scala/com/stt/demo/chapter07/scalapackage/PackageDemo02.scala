package com.atguigu.scala2

import scala.beans.BeanProperty

class Manager(var name: String) {
  //第一种形式 [使用相对路径引入包]
  @BeanProperty var age: Int = _
  //第二种形式, 和第一种一样，都是相对路径引入
  @scala.beans.BeanProperty var age2: Int = _
  //第三种形式, 是绝对路径引入，可以解决包名冲突
  @_root_.scala.beans.BeanProperty var age3: Int = _
}

object TestBean {
  def main(args: Array[String]): Unit = {
    val m = new Manager("jack")
    println("m=" + m)
  }
}
