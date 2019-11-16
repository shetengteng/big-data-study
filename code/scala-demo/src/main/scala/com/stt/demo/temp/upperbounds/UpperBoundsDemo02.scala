package com.atguigu.temp.upperbounds

object UpperBoundsDemo02 {
  def main(args: Array[String]): Unit = {

  }
}

object LowerBoundsDemo {
  def main(args: Array[String]): Unit = {
    biophony(Seq(new Earth)).map(_.sound())
    biophony(Seq(new Animal)).map(_.sound())
    val res = biophony(Seq(new Bird))
    println("xxxx")
    res.map(_.sound())

  }

  def biophony[T >: Animal](things: Seq[T]) = things
}

class Earth { //Earth 类
  def sound() { //方法
    println("hello !")
  }
}

class Animal extends Earth {
  override def sound() = { //重写了Earth的方法sound()
    println("animal sound")
  }
}

class Bird extends Animal {
  override def sound() = { //将Animal的方法重写
    println("bird sounds")
  }
}

class Moon {

}



