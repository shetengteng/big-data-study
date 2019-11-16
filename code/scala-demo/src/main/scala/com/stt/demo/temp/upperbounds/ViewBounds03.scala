package com.atguigu.temp.upperbounds

object ViewBounds03 {
  def main(args: Array[String]): Unit = {
    val p1 = new Person3("jack2",10)
    val p2 = new Person3("tom2",20)
    import MyImplicit.person2toOrderedPerson3
    val compareComm3 = new CompareComm3(p1,p2)
    println(compareComm3.getter.name)
  }
}


class Person3(val name: String, val age: Int)

class CompareComm3[T <% Ordered[T]](obj1: T, obj2: T) {
  def getter = if (obj1 > obj2) obj1 else obj2

  def geatter2 = if (obj1.compareTo(obj2) > 0) obj1 else obj2
}
