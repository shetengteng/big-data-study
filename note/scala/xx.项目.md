# 客户信息管理系统

```scala
package com.stt.demo.customer.app
import com.stt.demo.customer.view.CustomerView

object CustomerCrm {
    def main(args: Array[String]): Unit = {
        new CustomerView().mainMenu()
    }
}
```

```scala
package com.stt.demo.customer.bean

class Customer {

    var id: Int = _
    var name: String = _
    var gender: Char = _
    var age: Short = _
    var tel: String = _
    var email: String = _

    def this(id: Int,name: String, gender: Char,age:Short,tel:String,email:String){
        this
        this.id = id
        this.name = name
        this.gender = gender
        this.age = age
        this.tel = tel
        this.email = email
    }

    def this(name: String, gender: Char,age:Short,tel:String,email:String){
        this(-1,name,gender,age,tel,email)
    }

    override def toString: String = {
        id+"\t\t"+name+"\t\t"+gender+"\t\t"+age+"\t\t"+tel+"\t\t"+email
    }
}
```

```scala
package com.stt.demo.customer.service

import com.stt.demo.customer.bean.Customer

import scala.collection.mutable.ArrayBuffer
import util.control.Breaks._


class CustomerService {

    var customerNum = 1

    val customers = ArrayBuffer(new Customer(1,"stt",'男',11,"11124","work@xx.com"))

    def list():ArrayBuffer[Customer] = {
        customers
    }

    def add(customer: Customer): Boolean ={
        customerNum += 1
        customer.id = customerNum
        customers.append(customer)
        true
    }

    def delete(id:Int): Boolean={
        var index = findIndexById(id)
        if(index != -1){
            customers.remove(index)
            true
        }else {
            false
        }
    }

    def findIndexById(id:Int) = {
        var re = -1
        breakable {
            for(i <- 0 until customers.length){
                if(customers(i).id == id){
                    re = i
                    break()
                }
            }
        }
        re
    }
}
```

```scala
package com.stt.demo.customer.view

import com.stt.demo.customer.bean.Customer
import com.stt.demo.customer.service.CustomerService

import scala.io.StdIn

class CustomerView {

    val customerService = new CustomerService()
    // 用于判断是否退出
    var loop = true
    // 用于接收用户定义的key
    var key = ' '

    def mainMenu():Unit = {

        do{
            println("-----------客户信息管理软件----------")
            println("           1 添 加 客 户 ")
            println("           2 修 改 客 户 ")
            println("           3 删 除 客 户 ")
            println("           4 客 户 列 表 ")
            println("           5 退       出 ")
            println("-----------请选择（1-5）：-----------")

            key = StdIn.readChar()

            key match {
                case '1' => {
                    println("添加客户")
                    addd()
                }
                case '2' => {
                    println("修改客户")
                }
                case '3' => {
                    println("删除客户")
                    delete()
                }
                case '4' => {
                    println("客户列表")
                    list()
                }
                case '5' => {
                    println("退出")
                    loop = false
                }
                case _ => println("输入错误")
            }

        }while(loop)
        println("已经退出...")
    }

    def list(): Unit ={
        println()
        println("-----------客户信息列表查看----------")
        println("编号\t\t姓名\t\t性别\t\t年龄\t\t电话\t\t邮箱")
        // 遍历for循环
        for(customer <- customerService.list()){
            // 重写customer的toString方法
            println(customer.toString)
        }
        println("-----------客户信息列表完成----------")
        println()
    }

    def addd(): Unit ={
        println()
        println("-----------客户信息添加页面----------")
        println("姓名：")
        var name = StdIn.readLine()
        println("性别：")
        var gender = StdIn.readChar()
        println("年龄：")
        var age = StdIn.readShort()
        println("电话：")
        var tel = StdIn.readLine()
        println("邮箱：")
        var email = StdIn.readLine()
        var customer = new Customer(name,gender,age,tel,email )
        customerService.add(customer)
        println("-----------客户信息添加完成----------")
        println()
    }

    def delete(): Unit ={
        println()
        println("-----------客户信息删除页面----------")
        println("要删除的编号：")
        var id = StdIn.readInt()
        if(id == -1){
            println("---编号不合法--")
            return
        }
        println("确认是否删除(Y/N)：")
        val choice = StdIn.readChar().toLower
        if(choice == 'y'){
            customerService.delete(id)
            println("-----------客户信息删除完成----------")
            return
        }
        println("-----------客户信息删除结束----------")
        println()
    }
}
```

