# Hadoop 编译源码（面试重点）

> 由于apache官方的jar包是32位的，运行在64位的主机上有问题，因此需要在本地编译，同时在序列化方面引入其他jar包也需要再次编译一下

### 准备工作

- 新准备一台虚拟机，用于编译源码，一般编译时间需要半个小时
- CentOS联网，需要连接外网，ping一下百度测试一下
- 采用==root用户==，避免出现其他意外
- 准备jar包
  - hadoop-2.7.2-src.tar.gz （注意这里是源码的压缩包）
  - jdk-8u144-linux-x64.tar.gz
  - apache-ant-1.9.9-bin.tar.gz（build工具，打包用的）
  - apache-maven-3.0.5-bin.tar.gz
  - protobuf-2.5.0.tar.gz（序列化的框架）



### jar包安装

#### <a href="#安装JDK">JDK安装，环境配置</a>

#### Maven安装与配置

```shell
# 进行解压
[ttshe@hadoop108 software]$ tar -zvxf apache-maven-3.0.5-bin.tar.gz -C /opt/module/
# 更改镜像地址，加快下载jar包速度
[ttshe@hadoop108 apache-maven-3.0.5]$ vim conf/settings.xml 

# 配置镜像
<mirrors>
	<mirror>
		<id>nexus-aliyun</id>
		<mirrorOf>central</mirrorOf>
		<name>Nexus aliyun</name>     
   		<url>
   			http://maven.aliyun.com/nexus/content/groups/public
   		</url>
   </mirror>        
</mirrors>

# 配置环境变量 
[root@hadoop108 ttshe]# vi /etc/profile
#MAVEN_HOME
export MAVEN_HOME=/opt/module/apache-maven-3.0.5
export PATH=$PATH:$MAVEN_HOME/bin

# 使配置生效 并验证
[root@hadoop108 ttshe]# source /etc/profile
[root@hadoop108 ttshe]# mvn -version
```



#### Ant安装与配置

```shell
[root@hadoop108 software]# tar -zvxf apache-ant-1.9.9-bin.tar.gz -C /opt/module/
# 修改环境变量，添加配置
[root@hadoop108 software]# vim /etc/profile
#ANT_HOME
export ANT_HOME=/opt/module/apache-ant-1.9.9
export PATH=$PATH:$ANT_HOME/bin
# 使配置生效
[root@hadoop108 software]# source /etc/profile
[root@hadoop108 software]# ant -version
Apache Ant(TM) version 1.9.9 compiled on February 2 2017
```



#### glibc-headers安装

```shell
[root@hadoop108 software]# yum install glibc-headers
```



#### g++安装

```shell
[root@hadoop108 software]# yum install gcc-c++
```



#### make安装

```shell
[root@hadoop108 software]# yum install make
```



#### cmake安装

```shell
[root@hadoop108 software]# yum install cmake
```



#### protobuf安装

```shell
[root@hadoop108 software]# tar -zvxf protobuf-2.5.0.tar.gz -C /opt/module/
# 进入protobuf的主目录，然后执行如下命令
[root@hadoop108 software]# cd /opt/module/protobuf-2.5.0/
[root@hadoop108 protobuf-2.5.0]# ./configure 
[root@hadoop108 protobuf-2.5.0]# make
[root@hadoop108 protobuf-2.5.0]# make check
[root@hadoop108 protobuf-2.5.0]# make install
[root@hadoop108 protobuf-2.5.0]# ldconfig
# 修改环境变量
[root@hadoop108 protobuf-2.5.0]# vi /etc/profile
#LD_LIBRARY_PATH
export LD_LIBRARY_PATH=/opt/module/protobuf-2.5.0
export PATH=$PATH:$LD_LIBRARY_PATH
[root@hadoop108 protobuf-2.5.0]# source /etc/profile
[root@hadoop108 protobuf-2.5.0]# protoc --version
libprotoc 2.5.0
```



#### openssl安装

```shell
[root@hadoop108 protobuf-2.5.0]# yum install openssl-devel
```



#### ncurses-devel 安装

```shell
[root@hadoop108 protobuf-2.5.0]# yum install ncurses-devel
```



### 编译源码

#### 执行命令

```shell
# 解压源码文件到/opt/目录
[root@hadoop108 software]# tar -zvxf hadoop-2.7.2-src.tar.gz -C /opt/
# 在解压后的文件夹下进行编译工作
[root@hadoop108 hadoop-2.7.2-src]# pwd
/opt/hadoop-2.7.2-src
[root@hadoop108 hadoop-2.7.2-src]# mvn package -Pdist,native -DskipTests -Dtar
```

等待30分钟左右，全部成功都是SUCCESS表示完成，否则需要重新执行该mvn命令

成功后64位的hadoop的包在/opt/hadoop-2.7.2-src/hadoop-dist/target下



#### 编码过程中常见的问题

- Maven install的时候JVM内存溢出

  - 处理方式：在环境配置文件和maven的执行文件均可调整MAVEN_OPT的heap大小。（详情查阅MAVEN 编译 JVM调优问题，如：http://outofmemory.cn/code-snippet/12652/maven-outofmemoryerror-method）

- 编译期间maven报错

  - 能网络阻塞问题导致依赖库下载不完整导致，多次执行命令（一次通过比较难）：[root@hadoop101 hadoop-2.7.2-src]#mvn package -Pdist,nativeN -DskipTests -Dtar

- 报ant，protobuf错误

  - 插件下载未完整或者插件版本问题，最开始连接有较多特殊情况，同时推荐2.7.0版本的问题汇总帖子 http://www.tuicool.com/articles/IBn63qf

