package com
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import java.net.InetAddress

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient


/**
  * Movie 数据集
  *
  * 260                                         电影ID，mid
  * Star Wars: Episode IV - A New Hope (1977)   电影名称，name
  * Princess Leia is captured and held hostage  详情描述，descri
  * 121 minutes                                 时长，timelong
  * September 21, 2004                          发行时间，issue
  * 1977                                        拍摄时间，shoot
  * English                                     语言，language
  * Action|Adventure|Sci-Fi                     类型，genres
  * Mark Hamill|Harrison Ford|Carrie Fisher     演员表，actors
  * George Lucas                                导演，directors
  */
case class Movie(mid: Int, name: String, descri: String, timelong: String, issue: String,
                 shoot: String, language: String, genres: String, actors: String, directors: String)

/**
  * Rating 数据集
  *
  * 1,31,2.5,1260759144
  */
case class Rating(uid: Int, mid: Int, score: Double, timestamp: Long)

/**
  * Tag 数据集
  *
  * 15,1955,dentist,1193435061
  */
case class Tag(uid: Int, mid: Int, tag: String, timestamp: Long)

// 把 MongoDB 和 Elasticsearch 的配置封装成样例类
/**
  * @param uri MongDB 的连接
  * @param db  MongDB 的 数据库
  */
case class MongoConfig(uri: String, db: String)

/**
  * @param httpHosts      ES 的 http 主机列表，逗号分隔
  * @param transportHosts ES 的 http 端口列表，逗号分隔
  * @param index          需要操作的索引库，即数据库
  * @param clusterName    集群名称：默认是 my-application
  */
case class ESConfig(httpHosts: String, transportHosts: String, index: String, clusterName: String)

object DataLoader {

  // 定义常量
  // 以 Window 下为例，需替换成自己的路径，linux 下为 /YOUR_PATH/resources/movies.csv
  val MOVIE_DATA_PATH = "D:\\learn\\JetBrains\\workspace_idea\\MovieRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\movies.csv"
  val TATING_DATA_PATH = "D:\\learn\\JetBrains\\workspace_idea\\MovieRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\ratings.csv"
  val TAG_DATA_PATH = "D:\\learn\\JetBrains\\workspace_idea\\MovieRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\tags.csv"

  // 定义 MongoDB 数据库中的一些表名
  val MONGODB_MOVIE_COLLECTION = "Movie"
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_TAG_COLLECTION = "Tag"

  // 定义 ES 中的一些索引（即数据库）
  val ES_MOVIE_INDEX = "Movie"

  // 主程序的入口
  def main(args: Array[String]): Unit = {
    // 定义用到的配置参数
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender",
      "es.httpHosts" -> "hadoop102:9200",
      "es.transportHosts" -> "hadoop102:9300",
      "es.index" -> "recommender",
      "es.cluster.name" -> "my-application"
    )

    // 创建一个 SparkConf 对象
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("DataLoader")

    // 创建一个 SparkSession 对象
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
      val sc=spark.sparkContext
    val input=sc.parallelize(Seq(("t1",1),("t2",1)))
    input.combineByKey(v=>(v,1),(acc:(Int,Int),newV)=>(acc._1+newV,acc._2+1),(acc1:(Int,Int),acc2:(Int,Int))=>(acc1._1+acc2._1,acc1._2+acc2._2))
input.combineByKey(v=>(v,1),(acc:(Int,Int),neV) =>(acc._1+neV,acc._2+1),(acc1:(Int,Int),acc2:(Int,Int))=>(acc1._1+acc2._1,acc1._2+acc2._2))
    // 在对 DataFrame 和 Dataset 进行许多操作都需要这个包进行支持
    import spark.implicits._

    // 加载数据，将 Movie、Rating、Tag 数据集加载进来
    // 数据预处理
    val movieRDD = spark.sparkContext.textFile(MOVIE_DATA_PATH)
    // 将 movieRDD 转换为 DataFrame
    val movieDF = movieRDD.map(
      item => {
        val attr = item.split("\\^")
        Movie(attr(0).toInt, attr(1).trim, attr(2).trim, attr(3).trim, attr(4).trim, attr(5).trim, attr(6).trim, attr(7).trim, attr(8).trim, attr(9).trim)
      }
    ).toDF()

    val ratingRDD = spark.sparkContext.textFile(TATING_DATA_PATH)
    // 将 ratingRDD 转换为 DataFrame
    val ratingDF = ratingRDD.map(
      item => {
        val attr = item.split(",")
        Rating(attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toLong)
      }
    ).toDF()

    val tagRDD = spark.sparkContext.textFile(TAG_DATA_PATH)
    // 将 tagRDD 装换为 DataFrame
    val tagDF = tagRDD.map(
      item => {
        val attr = item.split(",")
        Tag(attr(0).toInt, attr(1).toInt, attr(2).trim, attr(3).toLong)
      }
    ).toDF()

    // 声明一个隐式的配置对象
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))
    // 将数据保存到 MongoDB 中
    storeDataInMongDB(movieDF, ratingDF, tagDF)
    import org.apache.spark.sql.functions._
    // 数据预处理，把 movie 对应的 tag 信息添加进去，加一列，使用 “
    // ” 分隔：tag1|tag2|...
    /**
      * mid,tags
      * tags: tag1|tag2|tag3|...
      */
    val newTag = tagDF.groupBy($"mid")
      .agg(concat_ws("|", collect_set($"tag")).as("tags")) // groupby 为对原 DataFrame 进行打包分组，agg 为聚合(其操作包括 max、min、std、sum、count)
      .select("mid", "tags")
    // 将 movie 和 newTag 作 左外连接，把数据合在一起
    val movieWithTagsDF = movieDF.join(newTag, Seq("mid"), "left")

    // 声明一个隐式的配置对象
    implicit val esConfig = ESConfig(config("es.httpHosts"), config("es.transportHosts"), config("es.index"), config("es.cluster.name"))
    // 将数据保存到 ES 中
    storeDataInES(movieWithTagsDF)

    // 关闭 SparkSession
    spark.stop()
  }



  def storeDataInMongDB(movieDF: DataFrame, ratingDF: DataFrame, tagDF: DataFrame)(implicit mongoConfig: MongoConfig): Unit = {
    // 新建一个到 MongoDB 的连接
    val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))
    // 如果 MongoDB 中已有相应的数据库，则先删除
  mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).dropCollection()
    //将DF数据写入对应的人mongodb表中
    movieDF.write
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_TAG_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
    //对数据表建索引
    mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).createIndex(MongoDBObject("mid" ->1))
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("uid" ->1))
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("mid" ->1))
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("uid" ->1))
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("mid" ->1))
    mongoClient.close()
  }
  def storeDataInES(movieDF:DataFrame)(implicit eSConfig:ESConfig): Unit ={
    //新建es配置
    val settings:Settings=Settings.builder().put("cluster.name",eSConfig.clusterName).build()
    //新建一个es客户端
    val esClient =new PreBuiltTransportClient((settings))
    val REGEX_HOST_PORT="(.+):(\\d+)".r
    eSConfig.transportHosts.split(",").foreach{
      case REGEX_HOST_PORT(host:String,port:String)=>{
        esClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host),port.toInt))
      }
    }
    //先清理遗留的数据
    if(esClient.admin().indices().exists(new IndicesExistsRequest(eSConfig.index))
      .actionGet()
      .isExists)
      {
        esClient.admin().indices().create(new CreateIndexRequest(eSConfig.index))
        movieDF.write
          .option("es.nodes",eSConfig.httpHosts)
          .option("es.http.timeout","100m")
          .option("es.mapping.id","mid")
          .mode("overwrite")
          .format("org.elasticsearch.spark.sql")
          .save(eSConfig.index+"/"+ES_MOVIE_INDEX)
      }
  }

}