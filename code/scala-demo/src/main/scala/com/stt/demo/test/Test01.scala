package com.stt.demo.test

object Test01 {

  def main(args: Array[String]): Unit = {

//    println(signum(1))

//    var t = {}
//    println(t)
//    println(t.isInstanceOf[Unit])
//
//    for(i <- Range(10,-1,-1)){
//      println(i)
//    }
//
//    for(i <- 0 to 10 reverse){
//      println(i)
//    }



  }

  def signum(n: Int) = {
    if (n > 0) {
      1
    } else if (n == 0) {
      0
    } else {
      -1
    }
  }

  def countdown(n : Int): Unit ={
    for(i <- 0 to n reverse){
      println(i)
    }
  }

  def countdown2(n : Int): Unit ={
    (0 to n).reverse.foreach(println)
  }

}
