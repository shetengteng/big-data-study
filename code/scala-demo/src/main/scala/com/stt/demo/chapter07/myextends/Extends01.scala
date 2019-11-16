package com.atguigu.chapter07.myextends

object Extends01 {
  def main(args: Array[String]): Unit = {
    //使用
    val student = new Student
    student.name = "jack" //调用了student.name()
    student.studying()
    student.showInfo()
  }
}

class Person { //Person类
  var name : String = _
  var age : Int = _
  def showInfo(): Unit = {
    println("学生信息如下：")
    println("名字：" + this.name)
  }
}

//Student类继承Person
class Student extends Person {
  def studying(): Unit = {
    //这里可以使用父类的属性
    println(this.name + "学习 scala中....")
  }
}

