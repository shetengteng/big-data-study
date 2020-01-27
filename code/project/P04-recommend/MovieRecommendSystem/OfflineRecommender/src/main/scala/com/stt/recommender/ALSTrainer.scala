package com.stt.recommender

import breeze.numerics.sqrt
import com.stt.recommend.MongoConfig
import com.stt.recommender.OfflineRecommender.MONGODB_RATING_COLLECTION
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * 使用测试集合，选出最小的RMSE，得到最优的模型
  */
object ALSTrainer {

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

    val ratingRDD = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRating]
      .rdd
      .map {
        case MovieRating(uid, mid, score, timestamp) => Rating(uid, mid, score)
      }.cache()

    // 随机切分测试集和训练集
    val splits = ratingRDD.randomSplit(Array(0.8, 0.2))
    val trainingRDD = splits(0)
    val testRDD = splits(1)

    // 模型参数选择，输出最优参数
    adjustALSParam(trainingRDD, testRDD)

    spark.stop()
  }

  def adjustALSParam(trainingRDD: RDD[Rating], testRDD: RDD[Rating]) = {
    // 特征维度列表
    val K = Array(20, 50, 100)
    // 正则化取值
    // 这里只是测试，给的参数比较少
    val LAMBDA = Array(0.001, 0.01, 0.1)

    val result: Array[(Int, Double, Double)] =
      for (rank <- K; lambda <- LAMBDA) yield { // 在循环过程中的中间结果会进行缓存
        val model = ALS.train(trainingRDD, rank, 10, lambda) // 注意迭代次数不能太多，会有异常
        val rmse = calculateRMSE(model, testRDD)
        (rank, lambda, rmse)
      }

    //    println(result.sortBy(_._3).head)
    println(result.minBy(_._3))
  }

  /**
    * 计算当前的remse的返回数据 返回double
    *
    * @param model 模型
    * @param data  测试数据
    */
  def calculateRMSE(model: MatrixFactorizationModel, data: RDD[Rating]) : Double = {

    val userProducts = data.map {
      case Rating(user, product, score) => (user, product)
    }

    val predictRating: RDD[Rating] = model.predict(userProducts)

    // uid 和 mid作为外键，进行内连接
    val observed = data.map {
      case Rating(user, product, score) => ((user, product), score)
    }
    val predict = predictRating.map{
      case Rating(user, product, score) => ((user, product), score)
    }
    // 内连接得到(uid,mid),(actual,predict)
    val value: RDD[((Int, Int), (Double, Double))] = observed.join(predict)
    // 求平均数
    val mean: Double = value.map {
      case ((uid, mid), (actual, pre)) => {
        val err = actual - pre // 误差
        err * err
      }
    }.mean()
    // 开根号
    sqrt(mean)
  }

}