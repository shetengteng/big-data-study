# 练习1

- 在Scala REPL(read->evaluateion->print->loop)中，计算3的平方根,然后再对该值求平方。现在，这个结果与3相差多少？ 
  - 提示：scala.math 找相应的方法

```scala
scala> scala.math.sqrt(3)
res0: Double = 1.7320508075688772

scala> res0 * res0
res1: Double = 2.9999999999999996

scala> 3 - res1
res2: Double = 4.440892098500626E-16
```

- Scala语言的sdk是什么?
  - 开发工具包

- Scala环境变量配置及其作用

- Scala程序的编写、编译、运行步骤是什么? 能否一步执行?

- Scala程序编写的规则

- 简述：在配置环境、编译、运行各个步骤中常见的错误

- 如何检测一个变量是val还是var? 
  - 定义一个var或者val，通过是否可以重新赋值判断出来是否是val

- Scala允许你用数字去乘一个字符串，去REPL中试一下"crazy"*3。这个操作做什么？在Scaladoc中如何找到这个操作? 

```scala
scala> "crazy"*3
res3: String = crazycrazycrazy
```

![1571129267918](../img/scala/10.png)



- 10 max 2的含义是什么？max方法定义在哪个类中？ 
  - 返回2个数中的最大值

```scala
scala> 10 max 2
res4: Int = 10

scala> 10.max(2)
res6: Int = 10

scala> 10 max 88
res5: Int = 88
```

![1571129484151](../img/scala/11.png)

- 用BigInt计算2的1024次方 
  - 提示：在BigInt 找相应的方法 

```scala
scala> val t = BigInt(2)
// 使用BigInt的apply进行变量声明
t: scala.math.BigInt = 2

scala> t.pow(1024)
res7: scala.math.BigInt = 179769313486231590772930519078902473361797697894230657273430081157732675805500963132708477322407536021120113879871393357658789768814416622492847430639474124377767893424865485276302219601246094119453082952085005768838150682342462881473913110540827237163350510684586298239947245938479716304835356329624224137216

scala> BigInt(2).pow(1024)
res8: scala.math.BigInt = 179769313486231590772930519078902473361797697894230657273430081157732675805500963132708477322407536021120113879871393357658789768814416622492847430639474124377767893424865485276302219601246094119453082952085005768838150682342462881473913110540827237163350510684586298239947245938479716304835356329624224137216
```

- 在Scala中如何获取字符串“Hello”的首字符和尾字符？ 
  - 提示: 在String中找相应的方法

```scala
scala> "hello".take(0)
res9: String = ""

scala> "hello".take(1)
res10: String = h

scala> "hello".takeRight(0)
res11: String = ""

scala> "hello".takeRight(1)
res12: String = o

scala> "hello"(0)
res13: Char = h

scala> "hello".reverse.take(1)
res14: String = o
```

![1571129818313](../img/scala/12.png)

