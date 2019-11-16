package com.atguigu.temp.upperbounds

object ViewBounds02 {
  def main(args: Array[String]): Unit = {

    val jack = new Person("jack",10)
    val tom = new Person("tom",11)
    val compareComm2 = new CompareComm2(jack,tom)
    println(compareComm2.getter.name)


  }
}


class Person(val name: String, val age: Int) extends Ordered[Person]{
  override def compare(that: Person) = {
    this.age - that.age
  }
}

class CompareComm2[T <% Ordered[T]](obj1: T, obj2: T) {
  def getter = if (obj1 > obj2) obj1 else obj2

  def geatter2 = if (obj1.compareTo(obj2) > 0) obj1 else obj2
}

