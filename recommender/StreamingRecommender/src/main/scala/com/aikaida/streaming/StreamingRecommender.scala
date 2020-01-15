package com.aikaida.streaming
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI, MongoCollection}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
object ConnHelper extends Serializable{
  lazy val jedis =new Jedis("localhost")
  lazy  val mongoClient =MongoClient(MongoClientURI("mongodb://localhost:27017/recommender"))
}
case class MongoConfig(uri:String,db:String)
case class Rating(uid:Int,mid:Int,score:Double,timestamp:Int)
case class Movie(mid:Int,name:String,descri:String,timelong:String,issue:String,shoot:String,language:String,genres:String,actors:String,directors:String)
case class Recommendation(mid:Int,score:Double)
case class GenersRecommendation(genres:String,recs:Seq[Recommendation])
case class MovieRecs(mid:Int,recs:Seq[Recommendation])
object StreamingRecommender {
  val MONGODB_MOVIE_COLLECTION = "monggodb_movie_colleciton"
  val MOVIE_RECS = "MovieRecs"


  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://",
      "mongo.db" -> "recommender",
      "kafka.topic" -> "recommender"
    )
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("StreanmingRecommender")
    //创建一个SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    //拿到streaming context
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(2)) //batch duration

    import spark.implicits._
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))
    //加载电影相似度矩阵数据，把它广播出去
    val simMovieMatrix = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRecs]
      .rdd
      .map { movieRecs =>
        (movieRecs.mid, movieRecs.recs.map(x => (x.mid, x.score)).toMap)
      }
      .collectAsMap()
    val simMovieMatrixBroadCast = sc.broadcast(simMovieMatrix)
    //定义kafka连接参数
    val kafkaParam = Map(
      "bootstrap.servers" -> "localhost:9002",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserizlizer" -> classOf[StringDeserializer],
      "group.id" -> "recommender",
      "auto.offset.reset" -> "latest"
    )

    //通过kafak创建一个DStream
    var kafkaStream = KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String]
      (Array(config("kafka.topic")), kafkaParam))
    //把原始数据转换成评分流水
    val ratingStream = kafkaStream.map {
      msg =>
        val attr = msg.value().split("\\|")
        (attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
    }
    //继续做流失处理，核心实时算法部分
    ratingStream.foreachRDD {
      rdds =>
        rdds.foreach {
          case (uid, mid, score, timestamp) => {
            println("rating data coming")
            //从redis里获取当前最近的k次评分，保存成Array（mid,score）
            val userRecentlyRatings = getUserRecentlyRating(MAX_USER_RATINGS_NUM, uid, ConnHelper.jedis)
            //从相似度矩阵中取出当前电影最相似的N个电影，作为备份电影
            var candidateMovies = getTopSimMovies(MAX_SIM_MOVIES_NUM, mid, uid, simMovieMatrixBroadCast.value)
            //对每个被选电影，计算推荐优先级，得到当前用户的实时推荐列表Array(mid,score)
            val streamRecs = computeMoviesScores(candidateMovies, userRecentlyRatings, simMovieMatrixBroadCast.value)
            //把推荐数据保存到mongodb中
            saveDataToMongDB(uid, StreamRecs)
          }
        }
    }
    //开始接收和处理数据
    ssc.start()
    ssc.awaitTermination()
  }

  //从redis操作返回的是Java类，为了用map操作需要引入转换类
  import scala.collection.JavaConversions._

  def getUserRecentlyRating(num: Int, uid: Int, jedis: Jedis): Array[(Int, Double)] = {
    //从redis读取数据，用户评分数据保存在uid:UID为key的队列,value市MID：SCORE
    jedis.lrange("uid:" + uid, 0, num - 1).map {
      item => //具体每个评分又是以冒号分割的两个值
        val attr = item.split("\\:")
        (attr(0).trim.toInt, attr(1).trim.toDouble)
    }
      .toArray
  }

  /*

  获取跟当前电影做相似的被选电影
   */
  def getTopSimMovies(num: Int, mid: Int, uid: Int, simMovies: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]])(implicit mongoConfig: MongoConfig): Array[Int] = {
    //1.从相似度矩阵中拿到所有相似的电影
    val allSimMovies = simMovies.get(mid).toArray
    //2.从mongodb中查询用户已经看过的电影
    val ratingExist = ConnHelper.mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION)
      .find(MongoDBObject("uid" -> uid))
      .toArray
      .map {
        item => item.get("mid").toString.toInt
      }

    //3.把看过的过滤，得到输出列表
    allSimMovies.filter(x => ratingExist.contains(x._1))
      .sortWith(_._2 > _._2)
      .take(num)
      .map(x => x._1)

  }

  def getTopsSimMovies(candidateMovies: Array[Int], userRecentlyRatings: Array[(Int, Double)], simMovies: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]])
  : Array[Int] = {
    //定义一个ArrayBuffer,用于保存每一个被选电影的基础评分
    val scores = scala.collection.mutable.ArrayBuffer[(Int, Double)]()
    //定义一个HashMap,保存每一个备选电影的增强减弱因子
    val increMap = scala.collection.mutable.HashMap[Int, Int]()
    val decreMap = scala.collection.mutable.HashMap[Int, Int]()
    for (candidateMovie <- candidateMovies; userRecentlyRating <- userRecentlyRatings) {
      //拿到被选电影和最近评分电影的相似度
      val simScore = getMoviesSimScore(candidateMovie, userRecentlyRating._1, simMovies)
      if (simScore > 0.7) {
        //计算被选电影的基础推荐得分
        scores += ((candidateMovie, simScore * userRecentlyRating._2))
        if (userRecentlyRating._2 > 3) {
          increMap(candidateMovie) = increMap.getOrDefault(candidateMovie, 0) + 1
        } else {
          decreMap(candidateMovie) = decreMap.getOrDefault(candidateMovie, 0) + 1
        }
      }
    }
    //根据备选电影的mid做groupby,根据公式去求最后的推荐评分
    scores.groupBy(_._1).map {
      //groupBy之后得到的数据Map(mid ->ArrayBuffer[(mid,score)])
      case (mid, scoreList) =>
        (mid, scoreList.map(_._2).sum / scoreList.length + log(increMap.getOrDefault(mid, 1)) - log(decreMap.getOrDefault(mid, 1)))
    }.toArray


    //获取两个电影之间的相似度
    def getMoviesSimScore(mid1: Int, mid2: Int, simMovies: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]]): Double = {
      simMovies.get(mid1) match {
       //如果值传一个参数则用some
        case Some(sims) => sims.get(mid2) match {
          case Some(score) => score
          case None => 0.0
        }
        case None => 0.0
      }
    }

    //求一个数的对数，底数认为10
    def log(m: Int): Double = {
      val N = 10
      math.log(m) / math.log(N)
    }

   /* def saveDataToMongDB(uid: Int, streamRecs: Array[(Int, Double)])(implicit mongoConfig: MongoConfig): Unit = {
      //定义到StreamRecs表的链接
      val streamRecsCollection = ConnHelper.mongoClient(mongoConfig.db)(MONGODB_STREAM_RECS_COLLECTION)
      //如果表中已经有ui的对应的数据，则删除
      streamRecsCollection.findAndRemove(MongoDBObject("uid" -> uid))
      //将stream数据存入表中
      streamRecsCollection.insert(MongoDBObject("uid" -> uid, "recs" -> streamRecs.map(x => MongoDBObject("mid" -> x._1, "score" -> x._2))))
    }*/
    //将推荐结果存入到mongodb
    def saveDataMongoDB(uid:Int,recs:Array[(Int,Double)])(implicit mongoConfig: MongoConfig):Unit={
    //获取mongo中的表
      val streamingMovie=ConnHelper.mongoClient(mongoConfig.db)(MONGOFB_MOVIE)
      streamingMovie.findAndRemove(MongoDBObject("uid" -> uid))
      streamingMovie.insert(MongoDBObject("uid" ->uid,"recs" ->recs.map(x =>MongoDBObject("midx" ->x._1,"score" ->x._2))))

    }
  }

  }