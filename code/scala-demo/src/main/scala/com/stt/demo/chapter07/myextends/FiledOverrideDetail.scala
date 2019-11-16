package com.atguigu.chapter07.myextends

object FiledOverrideDetail {
  def main(args: Array[String]): Unit = {
    println("xxx")
    val bbbb = new BBBB
    // bbbb.name = "jack" 相当于调用了 父类的 name_$eq()
    // println(bbbb.name) 相当于调用了子类的 name()
    // 这样出现数据设置和数据获取不一致
  }
}

class AAAA {
  val name: String = "" // 底层会生成 public name() 和 public name_$eq()
}

class BBBB extends AAAA {
  override val name: String = "jj" // 底层会生成  public name()
}


