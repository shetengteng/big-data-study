// object 表示一个伴生对象，可简单的理解成一个对象
// HelloScala 对象的名称，底层对应的类名是HelloScala$
// 对象是HelloScala$类型的一个静态对象MODULE$
object HelloScala{
  def main(args: Array[String]): Unit = {
    println("hello scala");
  }
}