Linux查看内存、磁盘存储、io 读写、端口占用、进程等命令
1、查看内存：top
2、查看磁盘存储情况：df -h
3、查看磁盘IO读写情况：iotop（需要安装一下：yum install iotop）、iotop -o（直接查看输出比较高的磁盘读写程序）
4、查看端口占用情况：netstat -tunlp | grep 端口号
5、查看进程：ps aux



# Shell中单引号和双引号区别

- 在/home/atguigu/bin创建一个test.sh文件

```bash
[ttshe@hadoop102 bin]$ vim test.sh 
```

- 在文件中添加如下内容

```bash
#!/bin/bash
do_date=$1

echo '$do_date'
echo "$do_date"
echo "'$do_date'"
echo '"$do_date"'
echo `date`
```

- 查看执行结果

```bash
[ttshe@hadoop102 bin]$ test.sh 2019-02-10
$do_date
2019-02-10
'2019-02-10'
"$do_date"
2019年 05月 02日 星期四 21:02:08 CST
```

- 总结
  - 单引号不取变量值
  - 双引号取变量值
  - 反引号`，执行引号中命令
  - 双引号内部嵌套单引号，取出变量值
  - 单引号内部嵌套双引号，不取出变量值



# Linux&Shell相关总结

- Linux常用命令

| 序号 | 命令                          | 命令解释                               |
| ---- | ----------------------------- | -------------------------------------- |
| 1    | top                           | 查看内存                               |
| 2    | df -h                         | 查看磁盘存储情况                       |
| 3    | iotop                         | 查看磁盘IO读写(yum install iotop安装） |
| 4    | iotop -o                      | 直接查看比较高的磁盘读写程序           |
| 5    | netstat -tunlp \| grep 端口号 | 查看端口占用情况                       |
| 6    | uptime                        | 查看报告系统运行时长及平均负载         |
| 7    | ps  aux                       | 查看进程                               |

- Shell常用工具
  - awk、sed、cut、sort

