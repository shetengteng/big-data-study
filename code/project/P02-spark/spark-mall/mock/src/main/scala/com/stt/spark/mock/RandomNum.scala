package com.stt.spark.mock

import java.util.Random

import scala.collection.mutable

object RandomNum {

  def apply(fromNum:Int,toNum:Int): Int =  {
      fromNum+ new Random().nextInt(toNum-fromNum+1)
    }

  //  实现方法  在fromNum和 toNum之间的 多个数组拼接的字符串 共amount个
  //  用delimiter分割  canRepeat为false则不允许重复
  def multi(fromNum:Int,toNum:Int,amount:Int,delimiter:String,canRepeat:Boolean) ={

    if(toNum - fromNum + 1 < amount){
      throw new IllegalArgumentException("toNum - fromNum + 1 < amount")
    }
    var re = if(!canRepeat) mutable.Set[Int]() else mutable.ListBuffer[Int]()

    while(re.size < amount){
      re += RandomNum(fromNum,toNum)
    }
    re.map(_.toString).reduce(_+delimiter+_)
  }

  def main(args: Array[String]): Unit = {
    println(RandomNum.multi(1,5,2,",",true))
  }

}