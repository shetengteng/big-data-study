package com.stt.recommender

import com.mongodb.casbah.{MongoClient, MongoClientURI}
import redis.clients.jedis.Jedis

// 连接助手对象 序列化操作
object ConnHelper extends Serializable{
  lazy val jedis = new Jedis("hadoop102")
  lazy val mongoClient = MongoClient(MongoClientURI("mongodb://localhost:27017/recommender"))
}