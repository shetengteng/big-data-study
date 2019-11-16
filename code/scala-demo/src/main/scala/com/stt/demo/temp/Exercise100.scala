package com.atguigu.temp

object Exercise100 {
  def main(args: Array[String]): Unit = {
    val lines = List("atguigu han hello ", "atguigu han aaa aaa aaa ccc ddd uuu")

    val res1 = lines.flatMap(_.split(" "))
    println("res1=" + res1)

    val res2 = res1.map((_,1))

    println("res2=" + res2)

    val res3 = res2.groupBy(_._1)
    println("res3=" + res3)

    val res4 = res3.map(x=>(x._1,x._2.size))
    println("res4=" + res4)

    val res5 = res4.toList.sortBy(_._2).reverse
    println("res5=" + res5)

    val res6 = lines.flatMap(_.split(" ")).map((_,1)).groupBy(_._1).map(x=>(x._1,x._2.size)).toList.sortBy(_._2).reverse
    println("res6="+res6)
  }



}
