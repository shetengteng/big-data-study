package com.stt.recommender

import java.util

import com.mongodb.casbah.commons.MongoDBObject
import com.stt.recommend.MongoConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object StreamingRecommender {

  val MAX_USER_RATINGS_NUM = 20 // 选择k次评分
  val MAX_SIM_MOVIES_NUM = 20 // 选择相似列表中的电影个数

  val MONGODB_STREAM_RECS_COLLECTION = "StreamRecs"

  val MONGODB_RATING_COLLECTION = "Rating" // 用户评分表，历史评分信息，需要过滤用户已经看过的电影
  val MONGODB_MOVIE_RECS_COLLECTION = "MovieRecs" // 电影相似度列表

  def main(args: Array[String]): Unit = {

    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender",
      "kafka.topic" -> "recommender"
    )

    //创建SparkConf配置
    val sparkConf = new SparkConf()
      .setAppName("StreamingRecommender")
      .setMaster(config("spark.cores"))

    //创建SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val ssc = new StreamingContext(spark.sparkContext, Seconds(5)) // 批处理时间 5s

    import spark.implicits._
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // 加载电影相似度矩阵数据，通过广播变量，在每个executor中有个副本
    val simMovieMatrix: collection.Map[Int, Map[Int, Double]] = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_RECS_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRecs]
      .rdd
      .map {
        case MovieRecs(mid, items) =>
          (
            mid,
            items.map(m => (m.mid, m.score)).toMap // 将items转换成map，便于查询
          )
      }.collectAsMap()

    // 进行广播操作
    val simMovieMatrixBroadcast: Broadcast[collection.Map[Int, Map[Int, Double]]] =
      spark.sparkContext.broadcast(simMovieMatrix)

    val kafkaParam = Map(
      "bootstrap.servers" -> "hadoop102:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "recommender",
      "auto.offset.reset" -> "latest"
    )

    // 创建一个kafkaDStream
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] =
      KafkaUtils.createDirectStream[String, String](
        ssc,
        LocationStrategies.PreferConsistent, // 策略，偏向连续的策略
        ConsumerStrategies.Subscribe[String, String](Array(config("kafka.topic")), kafkaParam)
      )

    // 把原始数据 UID|MID|SCORE|TIMESTAMP 转换成评分流
    val ratingDStream: DStream[(Int, Int, Double, Long)] = kafkaDStream.map {
      msg =>
        val attr = msg.value().split("\\|")
        (attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toLong)
    }

    // 核心实时算法部分
    // 每次接收一段来自kafka的评分信息，进行处理
    ratingDStream.foreachRDD(rdds => {
      rdds.foreachPartition(items => {
        items.foreach {
          case (uid, mid, score, timestamp) => {
            println("rating data start ....")

            // 1.从redis中获取当前用户最近的K次评分，保存成Array[(mid,score)]
            val userRecentlyRatings = getUserRecentlyRating(uid)

            // 2.从相似矩阵中取出当前电影最相似的N个电影，作为备选列表 Array[mid]
            val candidateMovies = getTopSimMovies(mid, uid, simMovieMatrixBroadcast)

            // 3.对每个备选电影，计算推荐优先级，得到当前用户的实时推荐列表 Array[(mid,score)]
            val streamRecs = computeMovieScores(candidateMovies, userRecentlyRatings, simMovieMatrixBroadcast)

            // 4.把推荐数据保存到mongodb中
            saveDataToMongoDB(uid, streamRecs)

          }
        }
      })
    })

    ssc.start()

    println("streaming start....")
    ssc.awaitTermination()
  }

  /**
    * 从redis获取uid的最近k次评分
    *
    * @param uid
    * @return 格式 Array[(mid,score)]
    */
  def getUserRecentlyRating(uid: Int): Array[(Int, Double)] = {

    // 从redis中读取数据，使用List集合存储
    // 数据格式 key=uid:UID value=MID:SCORE ===> uid:11223   222:3.9
    val jedis = ConnHelper.jedis

    val values: util.List[String] = jedis.lrange("uid:" + uid, 0, MAX_USER_RATINGS_NUM - 1)

    // 由于返回的是java对象，需要进行隐式转换,才能进行map操作
    import scala.collection.JavaConversions._

    values.map(item => {
      val fields = item.split("\\:")
      (fields(0).trim.toInt, fields(1).trim.toDouble)
    }).toArray
  }

  /**
    * 获取备选电影，和当前电影最相似的num个电影
    *
    * @param mid       当前电影的mid
    * @param uid       当前用户uid
    * @param simMovies 电影的相似度矩阵
    * @return
    */
  def getTopSimMovies(mid: Int, uid: Int,
                      simMovies: Broadcast[collection.Map[Int, Map[Int, Double]]])
                     (implicit mongoConfig: MongoConfig): Array[Int] = {

    // 从相似度矩阵中获取所有相似电影
    val allSimMovies: Array[(Int, Double)] = (simMovies.value) (mid).toArray

    // 从mongo中获取已经看过的电影的mid
    val ratingExist: Array[Int] = ConnHelper.mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION)
      .find(MongoDBObject("uid" -> uid))
      .toArray
      .map(item => item.get("mid").toString.toInt)

    // 过滤已经看过的电影，得到输出列表
    allSimMovies
      .filter { case (mid, simScore) => !ratingExist.contains(mid) }
      .sortWith(_._2 > _._2)
      .take(MAX_SIM_MOVIES_NUM)
      .map(x => x._1)
  }

  /**
    * 计算电影的优先级
    *
    * @param candidateMovies         候选电影mid
    * @param userRecentlyRatings     用户最近k次评分 mid,score
    * @param simMovieMatrixBroadcast 电影相似度矩阵
    * @return
    */
  def computeMovieScores(candidateMovies: Array[Int],
                         userRecentlyRatings: Array[(Int, Double)],
                         simMovieMatrixBroadcast: Broadcast[collection.Map[Int, Map[Int, Double]]])
  : Array[(Int, Double)] = {

    // 定义一个ArrayBuffer 用于保存每一个备选电影的基础得分 mid,score
    var scores = ArrayBuffer[(Int, Double)]()

    // 保存每一个备选电影的增强减弱因子
    var increMap = mutable.HashMap[Int, Int]()
    var decreMap = mutable.HashMap[Int, Int]()

    //
    val simMovieMatrix: collection.Map[Int, Map[Int, Double]] = simMovieMatrixBroadcast.value

    for (mid <- candidateMovies; userRecentlyRating <- userRecentlyRatings) {

      // 获取2个电影的相似度
      val simScore = getMovieSimScore(mid, userRecentlyRating._1)
      if (simScore > 0.7) {
        val Rr = userRecentlyRating._2
        // 计算基础得分
        scores.append((mid, simScore * Rr))
        // 对增强和减弱因子进行操作
        if (Rr > 3) {
          increMap(mid) = increMap.getOrElse(mid, 0) + 1
        } else {
          decreMap(mid) = decreMap.getOrElse(mid, 0) + 1
        }
      }
    }

    /**
      * 计算相似度，2个电影的相似度
      * @param mid1
      * @param mid2
      */
    def getMovieSimScore(mid1: Int, mid2: Int): Double = {
      simMovieMatrix.get(mid1) match {
        case Some(sims) => sims.get(mid2) match{
          case Some(score) => score
          case None => 0.0
        }
        case None => 0.0
      }
    }

    /**
      * 求m的对数，底数是10
      * @param m
      * @return
      */
    def log(m: Int): Double = {
//      math.log10(m)
      val N = 10
      math.log(m) / math.log(N)
    }

    // 对计算的score对mid做groupBy,依据公式计算最后的推荐得分
    scores
      .groupBy(_._1)
      .map {
        case (mid, scoreList) => (mid, scoreList.map(x => x._2).sum / scoreList.length + log(increMap.getOrElse(mid, 1)) - log(decreMap.getOrElse(mid, 1)))
      }
      .toArray
      .sortWith(_._2 > _._2)
  }

  def saveDataToMongoDB(uid: Int, streamRecs: Array[(Int, Double)])(implicit mongoConfig: MongoConfig) = {
    // 定义到StreamRecs
    val collection = ConnHelper.mongoClient(mongoConfig.db)(MONGODB_STREAM_RECS_COLLECTION)
    // 如果表中已有uid对应的数据，删除
    collection.findAndRemove(MongoDBObject("uid"->uid))
    collection.insert(
      MongoDBObject(
        "uid"->uid,
        "recs"-> streamRecs.map{
          case (mid, finalScore) => MongoDBObject("mid"->mid,"score"->finalScore)
        }
      )
    )
  }

}