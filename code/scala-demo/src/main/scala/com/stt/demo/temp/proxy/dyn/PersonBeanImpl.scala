package com.atguigu.temp.proxy.dyn

class PersonBeanImpl extends PersonBean {

  var name = ""
  var gender = ""
  var interests = ""
//  var rating: Int = _ //评分值
//  var ratingcount: Int = _ //评分次数
  var score :Int = _ // 评分值


   override def getName(): String = {
    return name
  }

   override def getGender(): String = {
    gender
  }

   override def getInterests(): String = {
    interests
  }

  //  override def getHotOrNotRating(): Int = {
  //    //rating/ratingcount 是平均分
  //    if (ratingcount == 0) return 0 else rating / ratingcount
  //
  //  }


   override def setName(name: String): Unit = {
    this.name = name
  }

   override def setGender(gender: String): Unit = {
    this.gender = gender
  }

   override def setInterests(interests: String): Unit = {
    this.interests = interests
  }

  //  override def setHotOrNotRating(rating: Int): Unit = {
  //    this.rating = rating
  //    ratingcount += 1
  //  }

  //  override def toString: String = {
  //    "什么意思"
  //  }
   override def getScore(): Int = {
    score
  }

   override def setScore(score: Int): Unit = {
    this.score = score
  }
}
