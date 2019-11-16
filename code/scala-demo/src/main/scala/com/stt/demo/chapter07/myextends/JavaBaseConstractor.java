package com.atguigu.chapter07.myextends;

public class JavaBaseConstractor {
    public static void main(String[] args) {

        //1.A()
        //2.B()
        B b = new B();


        //1.A(String name) jack
        //2.B(String name) jack
        B b2 = new B("jack");

    }
}

class A {
    public A() {
        System.out.println("A()");
    }
    public A(String name) {
        System.out.println("A(String name)" + name);
    }
}
class B extends A{
    public B() {
        //这里会隐式调用super(); 就是无参的父类构造器A()
        //super();
        System.out.println("B()");
    }
    public B(String name) {
        super(name);
        System.out.println("B(String name)" + name);
    }
}

