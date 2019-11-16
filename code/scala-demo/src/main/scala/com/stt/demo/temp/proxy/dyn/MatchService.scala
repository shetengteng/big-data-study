package com.atguigu.temp.proxy.dyn

import java.lang.reflect.Proxy

class MatchService {
  val tom = getPersonInfo("tom", "男", "爱好编程")

  val OwnerProxy = getOwnerProxy(tom)


  println("Name is " + OwnerProxy.getName())
  println("Interests is " + OwnerProxy.getInterests())

  OwnerProxy.setInterests("爱好淘宝~~")
  println("Interests is " + OwnerProxy.getInterests())
  //自己给自己设置评分，通过代理控制，不能成功
  OwnerProxy.setScore(100)
  println("Score is " + OwnerProxy.getScore()) //分值仍然为0



  println("****测试NonOwnerInvocationHandler**********")

  val mary = getPersonInfo("mary", "女", "爱好购物...")

  val nonOwnerProxy = getNonOwnerProxy(mary)
  println("Name is " + nonOwnerProxy.getName())
  println("Interests is " + nonOwnerProxy.getInterests())
  //其它人不能修改兴趣，通过代理进行控制不能调用setInterests
  nonOwnerProxy.setInterests("爱好小猫咪~~")
  println("Interests is " + nonOwnerProxy.getInterests())
  nonOwnerProxy.setScore(68)//其它人可以评分
  println("score is " + nonOwnerProxy.getScore())

  def getPersonInfo(name: String, gender: String, interests: String): PersonBean = {
    val person = new PersonBeanImpl()
    person.setName(name)
    person.setGender(gender)
    person.setInterests(interests)
    person
  }

  def getOwnerProxy(person: PersonBean): PersonBean = {


    Proxy.newProxyInstance(person.getClass().getClassLoader(), person.getClass().getInterfaces(), new OwnerInvocationHandler(person)).asInstanceOf[PersonBean]

  }

  def getNonOwnerProxy(person: PersonBean): PersonBean = {
    Proxy.newProxyInstance(person.getClass()
      .getClassLoader(), person.getClass().getInterfaces(),
      new NonOwnerInvocationHandler(person)).asInstanceOf[PersonBean]
  }
}
