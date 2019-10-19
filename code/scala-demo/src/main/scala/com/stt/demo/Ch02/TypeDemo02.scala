package com.stt.demo.Ch02

object TypeDemo02 {
  def main(args: Array[String]): Unit = {

    // 在scala中仍然遵守，低精度的值，向高精度的值自动转换(implicit conversion) 隐式转换
    var num = 1.2 //默认为double
    var num2 = 1.7f //这是float
    //num2 = num ,error ,修改num2 = num.toFloat

    println(sayHello)
  }

  //比如开发中，我们有一个方法，就会异常中断，这时就可以返回Nothing
  //即当我们Nothing做返回值，就是明确说明该方法没有没有正常返回值
  def sayHello: Nothing = {
      throw  new Exception("抛出异常")
  }
}