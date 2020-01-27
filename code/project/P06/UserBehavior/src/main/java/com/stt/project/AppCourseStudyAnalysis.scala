package com.stt.project

import org.apache.spark.sql.SparkSession

/**
  * 指标1，统计观看视频和完成视频人数
  */
object AppCourseStudyAnalysis {

  def main(args: Array[String]): Unit = {

    // 获取日期参数
    val day = args(0)
    if("".equals(day) || day.length == 0){
      println("usage:please input date,eg:20190402")
      System.exit(1)
    }

    val session: SparkSession = SparkSession.builder()
        .appName(getClass.getSimpleName)
        .config("spark.sql.warehouse.dir","/user/hive/warehouse")
        .enableHiveSupport()
        .getOrCreate()

    import session.sql

    // 创建dws表，注意在spark sql中不能有;
    sql(s"""
           |create table if not exists tmp.app_cource_study_analysis_${day}(
           |    watch_video_count INT,
           |    complete_video_count INT,
           |    dt INT
           |) row format delimited fields terminated by '\t'
       """.stripMargin)

    // 将分析的结果插入dws表
    sql(s"""
           |insert overwrite table tmp.app_cource_study_analysis_${day}
           |select
           |  sum(watch_video_count),
           |  sum(complete_video_count),
           |  ${day} dt
           |from(
           |    select count(uid) as watch_video_count,0 as complete_video_count
           |    from (
           |        select uid
           |        from dwd.user_behavior
           |        where dt=${day} and event_key='startVideo'
           |        group by uid) t1
           |    union all
           |    select 0 as watch_video_count,count(uid) as complete_video_count
           |    from (
           |        select uid
           |        from dwd.user_behavior
           |        where dt=${day} and event_key='endVideo'
           |        and (end_video_time-start_video_time) >= video_length
           |        group by uid) t2
           |) t3
       """.stripMargin)

    session.stop()
  }
}