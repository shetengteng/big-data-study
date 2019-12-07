package com.stt.recommend

/**
  * tags 数据集
  * 15          uid
  * 1955        mid
  * dentist     tag
  * 1193435061  timestamp
  */
case class Tag(
                uid: Int,
                mid: Int,
                tag: String,
                timestamp: Long
              )