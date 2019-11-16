package com.atguigu.temp.proxy.rmi

import java.rmi.{Remote, RemoteException}

trait MyRemote extends Remote{
  //一个抽象方法
  @throws(classOf[RemoteException])
  def sayHello(): String //throws RemoteException

}
