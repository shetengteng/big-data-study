package com.atguigu.temp.upperbounds


//implicit def person2toOrderedPerson2 (p: Person2) = new Ordered[Person2] {
//  override def compare (that: Person2) = {
//  this.age - that.age
//}
//}


object MyImplicit {
  implicit def person2toOrderedPerson3(p:Person3) = new Ordered[Person3] {
    override def compare(that: Person3) = p.age - that.age
  }
}