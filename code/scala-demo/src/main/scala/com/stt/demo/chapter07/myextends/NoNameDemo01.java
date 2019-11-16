package com.atguigu.chapter07.myextends;

public class NoNameDemo01 {
    public static void main(String[] args) {
        //在java中去创建一个匿名子类对象
        A2 a2 = new A2() {
            @Override
            public void cry() {
                System.out.println("cry...");
            }
        };
        a2.cry();
    }
}

abstract class A2 {
    abstract public void cry();
}

