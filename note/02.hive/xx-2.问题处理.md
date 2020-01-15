- SecureCRT 7.3出现乱码或者删除不掉数据，免安装版的SecureCRT 卸载或者用虚拟机直接操作或者换安装版的SecureCRT 

# 连接不上mysql数据库

​	（1）导错驱动包，应该把mysql-connector-java-5.1.27-bin.jar导入/opt/module/hive/lib的不是这个包。错把mysql-connector-java-5.1.27.tar.gz导入hive/lib包下。

​	（2）修改user表中的主机名称没有都修改为%，而是修改为localhost

- hive默认的输入格式处理是CombineHiveInputFormat，会对小文件进行合并

```bash
hive (default)> set hive.input.format;
hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat
```

可以采用HiveInputFormat就会根据分区数输出相应的文件。

hive (default)> set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

- 不能执行mapreduce程序

​	可能是hadoop的yarn没开启。

- 启动mysql服务时，报MySQL server PID file could not be found! 异常。

​	在/var/lock/subsys/mysql路径下创建hadoop102.pid，并在文件中添加内容：4396

- 报service mysql status MySQL is not running, but lock file (/var/lock/subsys/mysql[失败])异常。

​	解决方案：在/var/lib/mysql 目录下创建： -rw-rw----. 1 mysql mysql        5 12月 22 16:41 hadoop102.pid 文件，并修改权限为 777



# JVM堆内存溢出

描述：java.lang.OutOfMemoryError: Java heap space

解决：在yarn-site.xml中加入如下代码

```xml
<property>
	<name>yarn.scheduler.maximum-allocation-mb</name>
	<value>2048</value>
</property>
<property>
  	<name>yarn.scheduler.minimum-allocation-mb</name>
  	<value>2048</value>
</property>
<property>
	<name>yarn.nodemanager.vmem-pmem-ratio</name>
	<value>2.1</value>
</property>
<property>
	<name>mapred.child.java.opts</name>
	<value>-Xmx1024m</value>
</property>
```



