import scala.collection.mutable.ListBuffer

object Test{
  def main(args: Array[String]): Unit = {
    val plus3 = (x: Int) => {
      println("plus3")
      x+3
    }
    println(plus3(1))

    val f1 = (n1:Int,n2:Int) => n1+n2
    println("f1的类型="+f1)
    println(f1(1,2))
  }
}

//plus3
//4
//f1的类型=<function2>
//3