package com.atguigu.temp.upperbounds

object ContextBoundsDemo {

  implicit val personComparetor = new Ordering[Person4] {
    override def compare(p1: Person4, p2: Person4): Int =
      p1.age - p2.age
  }

  def main(args: Array[String]): Unit = {
    val p1 = new Person4("mary", 30)
    val p2 = new Person4("smith", 35)

    val compareComm4 = new CompareComm4(p1, p2)

    println(compareComm4.greater.name)

    val compareComm5 = new CompareComm5(p1, p2)

    println(compareComm5.greater.name)

    //看看
    println("xxx" + personComparetor.hashCode())

    val compareComm6 = new CompareComm6(p1, p2)

    println("com6=" + compareComm6.geatter.name)

  }

}

//方式1

class CompareComm4[T: Ordering](obj1: T, obj2: T)(implicit comparetor: Ordering[T]) {
  def greater = if (comparetor.compare(obj1, obj2) > 0) obj1 else obj2
}


class CompareComm5[T: Ordering](obj1: T, obj2: T) {
  def greater = {
    def f1(implicit cmptor: Ordering[T]) = cmptor.compare(obj1, obj2)

    if (f1 > 0) obj1 else obj2
  }
}

//使用implicity语法糖


class CompareComm6[T: Ordering](o1: T, o2: T) {
  def geatter = {
    //这句话就是会发生隐式转换，获取到隐式值 personComparetor
    val comparetor = implicitly[Ordering[T]]
    println("CompareComm6 comparetor" + comparetor.hashCode())

    if (comparetor.compare(o1, o2) > 0) o1 else o2
  }
}


//一个普通的Person类
class Person4(val name: String, val age: Int) {
  override def toString = this.name + "\t" + this.age
}
