package com.atguigu.temp.proxy.rmi

import java.rmi.registry.LocateRegistry
import java.rmi.{Naming, RemoteException}
import java.rmi.server.UnicastRemoteObject

class MyRemoteImpl extends UnicastRemoteObject with MyRemote {
  @throws(classOf[RemoteException])
  override def sayHello(): String = {
    "Hello World!~"
  }
}

object MyRemoteImpl {
  def main(args: Array[String]): Unit = {
    val service: MyRemote = new MyRemoteImpl()
    //准备把服务绑定到9999端口
//    LocateRegistry.createRegistry(9999)
//    Naming.rebind("RemoteHello", service)
    Naming.rebind("rmi://127.0.0.1:9999/RemoteHello", service)
    println("远程服务开启，在127.0.0.1 的 9999端口监听，服务名 RemoteHello")
  }
}
