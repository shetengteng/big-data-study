# Sqoop参数

```bash
/opt/module/sqoop/bin/sqoop import \
--connect \
--username \
--password \
--target-dir \
--delete-target-dir \
--num-mappers \
--fields-terminated-by   \
--query   "$2" ' and $CONDITIONS;'
```



## Sqoop导入导出Null存储一致性问题

Hive中的Null在底层是以“\N”来存储，而MySQL中的Null在底层就是Null，为了保证数据两端的一致性。在导出数据时采用--input-null-string和--input-null-non-string两个参数。导入数据时采用--null-string和--null-non-string



## Sqoop数据导出一致性问题

- 先导出到mysql临时表，临时表成功后再向目标mysql表导入数据
- 场景1：如Sqoop在导出到Mysql时，使用4个Map任务，过程中有2个任务失败，那此时MySQL中存储了另外两个Map任务导入的数据，此时老板正好看到了这个报表数据。而开发工程师发现任务失败后，会调试问题并最终将全部数据正确的导入MySQL，那后面老板再次看报表数据，发现本次看到的数据与之前的不一致，这在生产环境是不允许的
- 官网：http://sqoop.apache.org/docs/1.4.6/SqoopUserGuide.html

```text
Since Sqoop breaks down export process into multiple transactions, it is possible that a failed export job may result in partial data being committed to the database. This can further lead to subsequent jobs failing due to insert collisions in some cases, or lead to duplicated data in others. You can overcome this problem by specifying a staging table via the --staging-table option which acts as an auxiliary table that is used to stage exported data. The staged data is finally moved to the destination table in a single transaction.
```

- –staging-table方式
  - --clear-staging-table 表示成功后清除临时表

```text
sqoop export --connect jdbc:mysql://192.168.137.10:3306/user_behavior --username root --password 123456 --table app_cource_study_report --columns watch_video_cnt,complete_video_cnt,dt --fields-terminated-by "\t" --export-dir "/user/hive/warehouse/tmp.db/app_cource_study_analysis_${day}" --staging-table app_cource_study_report_tmp --clear-staging-table --input-null-string '\N'
```

- 场景2：设置map数量为1个（不推荐，面试官想要的答案不只这个）
  - 多个Map任务时，采用–staging-table方式，仍然可以解决数据一致性问题



## Sqoop底层运行的任务是什么

- 只有Map阶段，没有Reduce阶段的任务

  

## Sqoop数据导出的时候一次执行多长时间

- Sqoop任务5分钟-2个小时的都有
- 取决于数据量