package com.atguigu.chapter07.scalapackage.importdetails

import scala.beans._ //_表示将该包的所有内容引入，等价 *
object TestImport {
  def main(args: Array[String]): Unit = {

  }
}

class User {
  import scala.beans.BeanProperty //在需要时才引入，作用域在{}
  @BeanProperty var  name : String = ""

  def test(): Unit = {
    //可以使用选择器，选择引入包的内容，这里，我们只引入 HashMap, HashSet
    import scala.collection.mutable.{HashMap, HashSet}
    var map = new HashMap()
    var set = new HashSet()
  }

  def test2(): Unit = {
    //下面的含义是 将 java.util.HashMap 重命名为 JavaHashMap
    import java.util.{ HashMap=>JavaHashMap, List}
    import scala.collection.mutable._
    var map = new HashMap() // 此时的HashMap指向的是scala中的HashMap
    var map1 = new JavaHashMap(); // 此时使用的java中hashMap的别名

  }

}
class Dog {

  @BeanProperty var  name : String = "" //可以吗?
}
