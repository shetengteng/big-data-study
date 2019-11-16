package com.atguigu.chapter07.myextends

object ScalaBaseConstrator {
  def main(args: Array[String]): Unit = {
    //分析一下他的执行流程
    //1.因为scala遵守先构建父类部分 extends Person700()
    //2.Person...

    //val emp = new Emp700()

    //分析一下他的执行流程
    //1.因为scala遵守先构建父类部分 extends Person700()
    //2.Person...
    //3.Emp .... (Emp700的主构造器)
    println("=====================")
    val emp2 = new Emp700("mary")


    println("**************************")
    //分析执行的顺序
    //1.Person...
    //2.默认的名字
    //3.Emp ....
    //4.Emp 辅助构造器~
    val emp3 = new Emp700("smith")

    println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")
    //再测试一把
    //Person.. , name= "terry"
    //Emp ....
    val emp4 = new Emp700("terry", 10)
    emp4.showInfo() // 雇员的名字 terry

  }
}

//父类Person
class Person700(pName:String) {
  var name = pName
  println("Person...")
  def this() {
    this("默认的名字")
    println("默认的名字")

  }
}

//子类Emp继承Person
class Emp700(eName:String,eAge:Int) extends Person700(eName) {

  println("Emp ....")
  //辅助构造器
  def this(name: String) {

    this(name,100) // 必须调用主构造器
    this.name = name
    println("Emp 辅助构造器~")
  }

  def showInfo(): Unit = {
    println("雇员的名字 ", name)
  }
}

