package com.stt.demo.Ch01

object Part02_Print {
  def main(args: Array[String]): Unit = {
    var str1 : String = "hello"
    var str2 : String = "world"
    println(str1+str2)

    var name : String = "tom"
    var age : Int = 10
    var sal : Float = 10.67f
    var height : Double = 100.89
    // 格式化输出
    printf("name=%s age=%d salary=%.2f height=%.3f",name,age,sal,height)

    // scala 支持使用$输出内容，编译器会自动解析$对应变量
    // 使用s开头 表示解析$，可以对变量进行运算
    println(s"\nname=$name age=$age salary=${sal+10} height=$height")
  }
}
