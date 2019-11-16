package com.atguigu.temp.upperbounds

import java.lang


object UpperBoundsDemo01 {
  def main(args: Array[String]): Unit = {
    val compareInt = new CompareInt(1, 2)
    println(compareInt.greater)

    val compareFloat = new CompareFloat(1.1f, 0.2f)
    println(compareFloat.greater)


    val commonCompare1 = new CommonCompare[Integer](Integer.valueOf(10),Integer.valueOf(30))
    println(commonCompare1.greater)

    val commonCompare2 = new CommonCompare[java.lang.Float](java.lang.Float.valueOf(1.1f),java.lang.Float.valueOf(2.36f))
    println(commonCompare2.greater)

    //这样写会方式隐式转换
    val commonCompare3 = new CommonCompare[java.lang.Float](1.4f,4.5f)
    println(commonCompare3.greater)



  }
}

//编写
class CompareInt(n1: Int, n2: Int) {
  def greater = if (n1 > n2) n1 else n2
}

class CompareFloat(n1: Float, n2: Float) {
  def greater = if (n1 > n2) n1 else n2
}

//使用上界搞定
class CommonCompare[T <: Comparable[T]](obj1: T, obj2: T) {

  def greater = if (obj1.compareTo(obj2) > 0) obj1 else obj2
}