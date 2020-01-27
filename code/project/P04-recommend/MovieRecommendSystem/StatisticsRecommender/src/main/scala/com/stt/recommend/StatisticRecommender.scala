package com.stt.recommend

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime, ZoneId}

import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * 电影得分
  * @param mid
  * @param score
  */
case class Recommendation(mid:Int, score:Double)

/**
  * 依据类别的电影得分
  * @param genres
  * @param recs
  */
case class GenresRecommendation(genres:String, recs:Seq[Recommendation])

object StatisticRecommender {

  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_MOVIE_COLLECTION = "Movie"

  //统计的表的名称
  val RATE_MORE_MOVIES = "RateMoreMovies"
  val RATE_MORE_RECENTLY_MOVIES = "RateMoreRecentlyMovies"
  val AVERAGE_MOVIES = "AverageMovies"
  val GENRES_TOP_MOVIES = "GenresTopMovies"

  def main(args: Array[String]): Unit = {

    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )

    //创建SparkConf配置
    val sparkConf = new SparkConf()
      .setAppName("StatisticsRecommender")
      .setMaster(config("spark.cores"))

    //创建SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    //加入隐式转换
    import spark.implicits._

    implicit val mongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))

    // 从mongoDB中获取数据
    val ratingDF = spark.read
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Rating]
      .toDF()

    val movieDF = spark.read
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_MOVIE_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movie]
      .toDF()

    // 创建ratings的视图
    ratingDF.createOrReplaceTempView("ratings_view")

    // todo 不同的推荐统计结果
    // 1.历史热门统计，历史评分数据最多, 与评分无关，最近热门可能评分不高
    val rateMoreMoviesDF = spark.sql("select mid, count(mid) as count from ratings_view group by mid")
    // 把结果写入对应的mongodb表中
    storeDataInMongo(rateMoreMoviesDF,RATE_MORE_MOVIES)

    // 2.近期热门统计，按照"yyyyMM"格式选取最近的评分数据，统计评分个数
    // 注册udf函数
    spark.udf.register("changeDate",(x: Int)=>{
      LocalDateTime
        .ofInstant(Instant.ofEpochMilli(x*1000L), ZoneId.systemDefault())
        .format(DateTimeFormatter.ofPattern("yyyyMM")).toInt
    })
    // 对数据进行预处理，去除uid
    val ratingOfYearMonth =
      spark.sql("select mid, score, changeDate(timestamp) as yearmonth from ratings_view")

    ratingOfYearMonth.createOrReplaceTempView("ratingOfMonth_view")
    // 从ratingOfMonth中查找电影在各个月份的评分，mid, count, yearmonth
    val rateMoreRecentlyMoviesDF =
      spark.sql("select mid, count(mid) as count, yearmonth from ratingOfMonth_view " +
        "group by yearmonth, mid order by yearmonth desc , count desc")

    storeDataInMongo(rateMoreRecentlyMoviesDF,RATE_MORE_RECENTLY_MOVIES)

    // 3.优质电影推荐，统计电影的平均得分
    val avgMovieDF = spark.sql("select mid, avg(score) as avg from ratings_view group by mid")
    storeDataInMongo(avgMovieDF,AVERAGE_MOVIES)

    // 4.各类别的top10统计
    // 定义所有类别，一般是从数据库中获取，如redis

    val genres = List("Action","Adventure","Animation","Comedy","Crime","Documentary","Drama","Family","Fantasy","Foreign","History","Horror","Music","Mystery"
      ,"Romance","Science","Tv","Thriller","War","Western")

    // 将平均分加入到movie表中，添加一列，inner join
    val movieWithScore: DataFrame = movieDF.join(avgMovieDF,"mid")

    // 转成RDD
    val genresRDD: RDD[String] = spark.sparkContext.makeRDD(genres)

    //方式1： 计算类别top10, 首先对类别和电影做笛卡尔积
    //找出 movie 的字段 genres 值包含genre
    //方式2： 使用爆炸函数
    val genresTopMovieDF: DataFrame = genresRDD.cartesian(movieWithScore.rdd)
      .filter {
        case (genre, row) => row.getAs[String]("genres").toLowerCase.contains(genre.toLowerCase)
      }.map {
        // 将整个数据集的数据量减小，生成RDD[String,Iter[mid,avg]]
        case (genre, row) => {
          (genre, (row.getAs[Int]("mid"), row.getAs[Double]("avg")))
        }
      }.groupByKey()
      .map {
        case (genre, items) =>
          GenresRecommendation(
            genre,
            items.toList.sortWith(_._2 > _._2).take(10).map(item => Recommendation(item._1, item._2))
          )
      }.toDF()

    storeDataInMongo(genresTopMovieDF,GENRES_TOP_MOVIES)

    spark.stop()
  }

  def storeDataInMongo(df: DataFrame, collectName: String)(implicit mongoConfig: MongoConfig): Unit= {

    val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))
    mongoClient(mongoConfig.db)(collectName).dropCollection

    df.write
      .option("uri",mongoConfig.uri)
      .option("collection",collectName)
      .mode(SaveMode.Overwrite)
      .format("com.mongodb.spark.sql")
      .save()
  }

}