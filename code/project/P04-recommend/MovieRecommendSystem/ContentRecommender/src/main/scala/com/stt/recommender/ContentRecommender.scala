package com.stt.recommender

import com.stt.recommend.{MongoConfig, Movie}
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.jblas.DoubleMatrix

/**
  * 定义一个基准推荐对象
  *
  * @param mid
  * @param score
  */
case class Recommendation(mid: Int, score: Double)

/**
  * 定义电影内容信息提取出的特征向量的电影相似度列表
  *
  * @param mid
  * @param recs
  */
case class MovieRecs(mid: Int, recs: Seq[Recommendation])

object ContentRecommender {

  val MONGODB_MOVIE_COLLECTION = "Movie"

  val CONTENT_MOVIE_RECS = "ContentMovieRecs"

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )

    //创建SparkConf配置
    val sparkConf = new SparkConf()
      .setAppName("ContentRecommender")
      .setMaster(config("spark.cores"))

    //创建SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // 加载数据
    val movieTagsDF: DataFrame = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movie]
      .map(
        // genres 是按照 | 分隔 ,分词器默认按照空格进行分隔
        // 提取mid name genres 作为原始的内容特征
        m => (m.mid, m.name, m.genres)
      )
      .toDF("mid","name","genres")
      .cache()

    // todo 从内容信息中提取电影特征向量
    // 核心部分，使用TF-IDF 从内容信息中提取电影特征向量
    // 创建一个分词器，默认按照空格分隔
    // 对DF进行操作，需要指定操作的列名
    val tokenizer: RegexTokenizer = new RegexTokenizer()
      .setPattern("\\|")
      .setInputCol("genres")
      .setOutputCol("words")

    // 对原始数据做数据转换，生成新的一列words,words中的大写单词转换为了小写
    val wordsData = tokenizer.transform(movieTagsDF)

    // 引入 HashingTF工具，将一个词语序列化成对应的词频
    // numFeatures hash计算时，给定的分桶个数，减少hash碰撞
    // 给一个预设的值，默认是2的18次方
    val hashingTF = new HashingTF()
      .setInputCol("words")
      .setOutputCol("rawFeatures")
      .setNumFeatures(50)

    val featurizedData: DataFrame = hashingTF.transform(wordsData)

    // 中间显示 truncate 表示不要压缩
    // |2 |Jumanji (1995) |Adventure|Children|Fantasy  |[adventure, children, fantasy]  |(50,[11,13,19],[1.0,1.0,1.0])
    featurizedData.show(truncate = false)
    // (50,[11,13,19],[1.0,1.0,1.0] ===>
    // 表示稀疏向量矩阵，50表示分桶的个数，[11,13,19] 表示在桶的索引，[1.0,1.0,1.0] 表示在桶内的个数
    // 如果分桶个数过少，那么可能不同的单词分到同一个桶中，桶内的个数>1, 尽可能使用全集的个数作为桶的大小，减少误差

    // 引入IDF工具，可以得到idf模型
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel: IDFModel = idf.fit(featurizedData)
    // 使用模型对原数据进行处理，得到文档中每个词的tf-idf, 作为新的特征向量
    val rescaledData: DataFrame = idfModel.transform(featurizedData)

    rescaledData.show(truncate = false)
    // features 可以看到权重的变化
    //    |rawFeatures      |features
    //    |(50,[27],[1.0])  |(50,[27],[0.7605551441254693])
    // 扩展：可通过详情进行分词，提取特征

    // ml的库主要的操作对象是DF
    // mllib库主要的操作对象是 RDD


    // features 内的元素是 SparseVector 稀疏向量类型
    // 将计算得到的50个特征值形成矩阵，没有特征值的补0
    val movieFeaturesRDD: RDD[(Int, DoubleMatrix)] = rescaledData.map(
      row => (row.getAs[Int]("mid"), row.getAs[SparseVector]("features").toArray)
    ).rdd.map(x => (x._1, new DoubleMatrix(x._2)))

//    movieFeaturesRDD.collect.foreach(println)

    // 获取movie的特征矩阵，做笛卡尔积
    val movieFeaturesCartesianRDD: RDD[((Int, DoubleMatrix), (Int, DoubleMatrix))] = movieFeaturesRDD.cartesian(movieFeaturesRDD)

    movieFeaturesCartesianRDD
      .filter{
        case(a, b) => a._1 != b._1 // 把自己与自己的进行过滤
      }
      .map{
        // 计算余弦相似度
        case((mid1, matrix1), (mid2, matrix2)) =>
          (
            mid1,
            ( mid2, matrix1.dot(matrix2) / (matrix1.norm2()*matrix2.norm2()) )
          )
      }
      .filter{
        case(mid1,(mid2,simScore)) => simScore > 0.6
      }
      .groupByKey()
      .map{
        case(mid:Int,items:Iterable[(Int,Double)]) => MovieRecs(
            mid,
            items.toList.sortWith(_._2>_._2).map{case (mid, simScore) => Recommendation(mid,simScore)}
          )
      }
      .toDF()
      .write
      .option("uri",mongoConfig.uri)
      .option("collection",CONTENT_MOVIE_RECS)
      .mode(SaveMode.Overwrite)
      .format("com.mongodb.spark.sql")
      .save()

    spark.stop()
  }
}