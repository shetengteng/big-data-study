package com.stt.recommender

import com.stt.recommend.MongoConfig
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.jblas.DoubleMatrix


object OfflineRecommender {

  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_MOVIE_COLLECTION = "Movie"

  // 推荐表的名称
  val USER_RECS = "UserRecs"
  val MOVIE_RECS = "MovieRecs"

  val USER_MAX_RECOMMENDATION = 20

  def main(args: Array[String]): Unit = {

    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )

    //创建SparkConf配置
    val sparkConf = new SparkConf()
      .setAppName("OfflineRecommender")
      .setMaster(config("spark.cores"))

    //创建SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // 从mongoDB中获取数据
    val ratingRDD = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRating]
      .rdd // als 算法中需要传入的参数是rdd
      .map(rating => (rating.uid, rating.mid, rating.score)) // 转化成RDD，去除时间戳
      .cache() // 后面需要笛卡尔积计算，需要持久化在内存中，性能提升

    // 所有的user和movie的笛卡尔积
    // 在评分中有uid，LFM依赖于历史数据，一个新用户没有评分，则会有冷启动问题
    // 用户量大的时候需要缓存
    val userRDD = ratingRDD.map(_._1).distinct().cache()

    // 有些 movie 本身没有评分，不做预测，做预测性价比不高
    val movieRDD = ratingRDD.map(_._2).distinct().cache()

    // 训练隐语义模型，R值
    val trainData = ratingRDD.map { case (uid, mid, score) => Rating(uid, mid, score) }

    // rank 是特征个数
    // 交替最小二乘法，需要传入迭代次数
    // 传入正则化系数
    // 可以传入seed 生成初始化矩阵
    val (rank, iterations, lambda) = (50, 5, 0.03)
    val model = ALS.train(trainData, rank, iterations, lambda)

    // 基于用户和电影的隐特质，计算预测评分，得到用户推荐列表
    // 计算user 和 movie的笛卡尔积，得到一个空矩阵
    val userMovies: RDD[(Int, Int)] = userRDD.cartesian(movieRDD)

    // 调用model的predict方法，做预测评分
    // 可以单独计算一个user和movie，也可以传入一个空矩阵
    val preRatings: RDD[Rating] = model.predict(userMovies)

    val userRecsDF = preRatings
      .filter {
        case Rating(uid, mid, score) => score > 0 // 过滤出评分大于0的项
      }
      .map {
        case Rating(uid, mid, score) => (uid, (mid, score))
      }
      .groupByKey()
      .map {
        case (uid, items) =>
          UserRecs(
            uid,
            items
              .toList
              .sortWith(_._2 > _._2)
              .take(USER_MAX_RECOMMENDATION)
              .map { case (mid, score) => Recommendation(mid, score) }
          )
      }
      .toDF()

    userRecsDF
      .write
      .option("uri", mongoConfig.uri)
      .option("collection", USER_RECS)
      .mode(SaveMode.Overwrite)
      .format("com.mongodb.spark.sql")
      .save()

    // 基于电影隐特质，计算相似度矩阵，得到每2个电影相似度列表
    val movieFeatures = model.productFeatures.map {
      case (mid, features) => (mid, new DoubleMatrix(features)) // 得到每个电影的特征的一维矩阵
    }

    // 给2个电影做笛卡尔积
    val movieDoubleMatrix: RDD[((Int, DoubleMatrix), (Int, DoubleMatrix))] =
      movieFeatures.cartesian(movieFeatures)
        .filter {
          case (a, b) => a._1 != b._1
        }

    val movieRecsDF: DataFrame = movieDoubleMatrix.map {
      case (m1, m2) => {
        val simScore = consinSim(m1._2, m2._2) // 计算余弦相似度
        (m1._1, (m2._1, simScore))
      }
    }.filter {
      case (mid1, (mid2, simScore)) => simScore > 0.6 // 过滤出相似度>0.6的
    }.groupByKey()
      .map {
        case (mid1, items) =>
          MovieRecs(
            mid1,
            items.toList.sortWith {
              case (a, b) => a._2 > b._2
            }.map {
              case (mid, simScore) => Recommendation(mid, simScore)
            }
          )
      }.toDF()

    movieRecsDF
      .write
      .option("uri", mongoConfig.uri)
      .option("collection", MOVIE_RECS)
      .mode(SaveMode.Overwrite)
      .format("com.mongodb.spark.sql")
      .save()

    spark.stop()

  }

  /**
    * 求向量余弦相似度 cos = m1.m2 / |m1|*|m2|
    * @param m1
    * @param m2
    * @return
    */
  def consinSim(m1: DoubleMatrix, m2: DoubleMatrix) = {

    // norm1 表示L1范数
    // norm2 表示L2范数 模长
    m1.dot(m2) / (m1.norm2() * m2.norm2())
  }
}