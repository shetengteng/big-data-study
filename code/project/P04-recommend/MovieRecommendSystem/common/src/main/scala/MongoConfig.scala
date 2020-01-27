package com.stt.recommend

/**
  *
  * @param uri mongodb 连接
  * @param db  mongodb 数据库
  */
case class MongoConfig(
                        uri: String,
                        db: String
                      )