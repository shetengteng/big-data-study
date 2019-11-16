package com.atguigu.temp.proxy.dyn

trait PersonBean {

   def getName(): String

   def getGender(): String

   def getInterests(): String

   def getScore(): Int

   def setName(name: String)

   def setGender(gender: String)

   def setInterests(interests: String)

   def setScore(score:Int)
}
