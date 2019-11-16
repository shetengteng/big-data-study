package com.atguigu.temp

object Temp {
  def main(args: Array[String]): Unit = {
    //冒泡
    //二分
    val array = Array(1,3,9,100,101)
//    println(array.mkString(" "))
//    bubbleSort(array)
//    println(array.mkString(" "))
    binaryFind(array,11,0,array.length-1)
  }
  def bubbleSort(arr:Array[Int]): Unit = {
    var temp = 0
    for(i <- 0 until arr.length - 1 ) {
      for (j <- 0 until arr.length - 1 - i ) {
        if (arr(j) > arr(j+1)) {
           temp = arr(j)
          arr(j) = arr(j+1)
          arr(j+1) = temp
        }
      }
    }
  }
  def binaryFind(arr:Array[Int],findVal:Int,left:Int,right:Int): Unit = {

    //中间的
    val midIndex = (left + right) / 2
    if (left > right) {
      println("没有")
      return
    }
    val midVal = arr(midIndex)

    if (midVal > findVal) {
      binaryFind(arr,findVal,left,midIndex-1)
    }else if (midVal < findVal) {
      binaryFind(arr,findVal,midIndex+1,right)
    }else {
      println("找到index=" + midIndex)
    }
  }
}



class Dog(name: String) {
  override def toString: String = {
    name
  }
}
