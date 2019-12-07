package com.stt.recommend

/**
  * ratings 数据集 uid,mid,score,timestamp
  * 1                 uid
  * 1029              mid
  * 3.0               score
  * 1260759179        timestamp
  */
case class Rating(
                   uid: Int,
                   mid: Int,
                   score: Double,
                   timestamp: Long
                 )