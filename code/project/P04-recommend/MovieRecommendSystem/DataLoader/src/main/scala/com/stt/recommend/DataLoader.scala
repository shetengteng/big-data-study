package com.stt.recommend

import java.net.InetAddress

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.TransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient


object DataLoader {

  val MONGODB_MOVIE_COLLECTION = "Movie"
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_TAG_COLLECTION = "Tag"

  val ES_MOVIE_INDEX = "Movie"

  def main(args: Array[String]): Unit = {

    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender",
      "es.httpHosts" -> "hadoop102:9200",
      "es.transportHosts" -> "hadoop102:9300",
      "es.index" -> "recommender",
      "es.cluster.name" -> "my-es"
    )

    // 创建一个sparkConf
    val sparkConf: SparkConf = new SparkConf()
      .setMaster(config("spark.cores"))
      .setAppName("DataLoader")

    // 创建一个sparkSession
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    // 加载数据
    val movieRDD = spark.sparkContext.textFile(
      Thread.currentThread().getContextClassLoader.getResource("movies.csv").getPath)

    val movieDF = movieRDD.map(item =>{
      val fields = item.split("\\^")
      Movie(
        fields(0).toInt,
        fields(1).trim,
        fields(2).trim,
        fields(3).trim,
        fields(4).trim,
        fields(5).trim,
        fields(6).trim,
        fields(7).trim,
        fields(8).trim,
        fields(9).trim
      )
    }).toDF()

    val ratingRDD = spark.sparkContext.textFile(
      Thread.currentThread().getContextClassLoader.getResource("ratings.csv").getPath)

    val ratingDF = ratingRDD.map(item=>{
        val fields = item.split(",")
        Rating( fields(0).toInt, fields(1).toInt, fields(2).toDouble, fields(3).toLong)
      }).toDF()

    val tagRDD = spark.sparkContext.textFile(
      Thread.currentThread().getContextClassLoader.getResource("tags.csv").getPath)

    val tagDF = tagRDD.map(item=>{
      val fields = item.split(",")
      Tag( fields(0).toInt, fields(1).toInt, fields(2).trim, fields(3).toLong)
    }).toDF()

    // 将数据保存到mongoDB
    // 隐式类
    implicit val mongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))

    storeDataInMongo(movieDF, ratingDF, tagDF)

    // 数据预处理
    // 把movie对应的tag信息加入到movie的一列 方便ES做查询
    // mid tag1|tag2|tag3...

    import org.apache.spark.sql.functions.{concat_ws,collect_set}
    // 使用DSL语句操作，将tag组合成tags
    val newTag = tagDF.groupBy($"mid")
      .agg(
        concat_ws("|",collect_set($"tag"))
          .as("tags")
      ).select("mid","tags")

    // 将movie和 newTag进行join 左外连接 ,数据合并在一起
    val movieWithTagDF = movieDF.join(newTag,Seq("mid"),"left")

    // 隐式类
    implicit val esConfig = ESConfig(
      config("es.httpHosts"),
      config("es.transportHosts"),
      config("es.index"),
      config("es.cluster.name")
    )

    // 将数据保存到ES
    storeDataInES(movieWithTagDF)

    // 关闭
    spark.stop()
  }

  def storeDataInMongo(movieDF: DataFrame, ratingDF: DataFrame, tagDF: DataFrame)(implicit mongoConfig: MongoConfig): Unit = {

    // 新建一个mongodb的连接
    val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))

    // 如果mongo中已经有了相应的数据库，先删除
    mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).dropCollection
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).dropCollection
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).dropCollection

    // 将DF数据写入到相应的mongodb中
    movieDF.write
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_MOVIE_COLLECTION)
      .mode(SaveMode.Overwrite)
      .format("com.mongodb.spark.sql")
      .save()

    ratingDF.write
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_RATING_COLLECTION)
      .mode(SaveMode.Overwrite)
      .format("com.mongodb.spark.sql")
      .save()

    tagDF.write
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_TAG_COLLECTION)
      .mode(SaveMode.Overwrite)
      .format("com.mongodb.spark.sql")
      .save()

    // 建立索引
    mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).createIndex(MongoDBObject("mid"->1))
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("uid"->1))
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("mid"->1))
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("mid"->1))
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("uid"->1))

    mongoClient.close()
  }

  def storeDataInES(movieWithTagDF: DataFrame)(implicit eSConfig: ESConfig) = {
    // 新建es配置
    val settings: Settings = Settings.builder()
      .put("cluster.name",eSConfig.clustername).build()

    // 新建es客户端
    val esClient = new PreBuiltTransportClient(settings)

    val REGEX_HOST_PORT = "(.+):(\\d+)".r
    eSConfig.transportHosts.split(",").foreach{
      case REGEX_HOST_PORT(host:String, port:String) =>{
        // 配置addTransportAddress
        esClient.addTransportAddress(
          new TransportAddress(InetAddress.getByName(host),port.toInt)
        )
      }
    }
    // 清理遗留数据
    if(
        esClient.admin().indices().exists(new IndicesExistsRequest(eSConfig.index))
          .actionGet() // 发送get请求
          .isExists
      ){
        esClient.admin().indices().delete(new DeleteIndexRequest(eSConfig.index))
    }

    // 创建索引
    esClient.admin().indices().create(new CreateIndexRequest(eSConfig.index))

    // 将数据写入到索引
    movieWithTagDF.write
      .option("es.nodes",eSConfig.httpHosts)
      .option("es.http.timeout","100m")
      .option("es.mapping.id","mid")
      .option("es.nodes.wan.only","true")
      .mode(SaveMode.Overwrite)
      .format("org.elasticsearch.spark.sql")
      .save(eSConfig.index + "/" + ES_MOVIE_INDEX)
  }

}