package com.atguigu.temp.upperbounds

import com.atguigu.temp.upperbounds.SeasonEm.SeasonEm

object GenericUse2 {
  def main(args: Array[String]): Unit = {

    val class1 = new EnglishClass[SeasonEm, String, String](SeasonEm.spring, "001班", "高级班")
    println(class1.classSeason + " " + class1.className + " " + class1.classType)

    val class2 = new EnglishClass[SeasonEm, String, Int](SeasonEm.spring, "002班", 1)
    println(class2.classSeason + " " + class2.className + " " + class2.classType)
  }
}

// Scala 枚举类型
object SeasonEm extends Enumeration {
  type SeasonEm = Value //自定义SeasonEm，是Value类型,这样才能使用
  val spring, summer, winter, autumn = Value
}

// 定义一个泛型类
class EnglishClass[A, B, C](val classSeason: A, val className: B, val classType: C)
