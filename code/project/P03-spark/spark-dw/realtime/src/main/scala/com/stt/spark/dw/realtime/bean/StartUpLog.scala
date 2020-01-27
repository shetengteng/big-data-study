package com.stt.spark.dw.realtime.bean


case class StartUpLog(var mid:String,
                      var uid:String,
                      appid:String,
                      area:String,
                      os:String,
                      ch:String,
                      logType:String,
                      vs:String,
                      var logDate:String, // 由于要将数存储到ES中，但是ES中没有转换函数，因此需要将ts转换为Hour等
                      var logHour:String,
                      var logHourMinute:String,
                      var ts:Long
                  )