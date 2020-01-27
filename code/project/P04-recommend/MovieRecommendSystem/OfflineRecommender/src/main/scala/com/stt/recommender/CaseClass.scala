package com.stt.recommender



/**
  * 为了与mllib中的Rating冲突区别开来，重新命名MovieRating
  */
case class MovieRating(uid: Int, mid: Int, score: Double, timestamp: Long)

/**
  * 定义一个基准推荐对象
  *
  * @param mid
  * @param score
  */
case class Recommendation(mid: Int, score: Double)

/**
  * 定义基于预测评分的用户推荐列表
  *
  * @param uid
  * @param recs
  */
case class UserRecs(uid: Int, recs: Seq[Recommendation])


/**
  * 定义基于LFM电影特征向量的电影相似度
  *
  * @param mid
  * @param recs
  */
case class MovieRecs(mid: Int, recs: Seq[Recommendation])




