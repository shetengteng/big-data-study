package com.atguigu.temp.generic

import com.atguigu.temp.generic.SeasonEnum.{SeasonEnum}

object GenericDemo02 {
  def main(args: Array[String]): Unit = {

    val class01 = new EnglishClass[SeasonEnum,String,String](SeasonEnum.spring,"001班","高级班")
    val class02 = new EnglishClass[SeasonEnum,String,Int](SeasonEnum.spring,"001班",1)
    println(class01.classSeason + " " + class01.className + " " + class01.classType)
    println(class02.classSeason + " " + class02.className + " " + class02.classType)


  }
}

object SeasonEnum extends Enumeration {
  type SeasonEnum = Value
  val spring = Value
  val summer = Value
  val autumn = Value
  val winter = Value
}

class EnglishClass[A, B, C](val classSeason: A, val className: B, val classType: C)
