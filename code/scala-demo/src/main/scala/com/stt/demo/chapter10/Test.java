package com.atguigu.chapter10;

public class Test {
    public static void main(String[] args) {
        BB bb = new BB();
        AA aa = bb;
        CC cc = new CC();
        aa = cc;
    }
}

interface AA {

}

class BB implements AA{

}

class CC implements AA{

}
