package com.atguigu.staticsticsRecommender

import java.sql.Date
import java.text.SimpleDateFormat
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
//导入MongDB Config
case class MongConfig(uri:String,db:String)
case class Rating(uid:Int,mid:Int,score:Double,timestamp:Int)
case class Movie(mid:Int,name:String,descri:String,timelong:String,issue:String,shoot:String,language:String,genres:String,actors:String,directors:String)
case class Recommendation(mid:Int,score:Double)
case class GenersRecommendation(genres:String,recs:Seq[Recommendation])
object StaticsAlgorithm {
  /**
    * 定义表明
    *
    */
  val MONGOODB_MOVIE_COLLECTION="Movie"
  val MONGODB_RATING_COLLECTION="Rating"
  val RATE_MOVIES_COLLECTION=" RateMoreMovies"
  val RATE_MORE_RECENTLY_MOVIES="RateMoreRecentlyMovies"
  val AVERAGE_MOVIES="average_movies"
  val GENRES_TOP_MOVIES="gentes_top_movies"


  def main(args:Array[String]): Unit ={
    val config =Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )
      //创建一个sparkconf
    val sparkConf =new SparkConf().setMaster(config("spark.cores")).setAppName("DataLoader")
    //创建一个SparkSession
    val spark=SparkSession.builder().config(sparkConf).getOrCreate()
     import spark.implicits._
    implicit val mongConfig=MongConfig(config("mongo.uri"),config("mongo.db"))
    //从mongodb加载数据
    val ratingDF =spark.read
      .option("uri",mongConfig.uri)
      .option("collection",MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Rating]
      .toDF()
    val movieDF=spark.read
      .option("uri",mongConfig.uri)
      .option("collection",MONGOODB_MOVIE_COLLECTION)
      .load()
      .as[Movie]
      .toDF()
    //创建名为rating的临时表
    ratingDF.createOrReplaceTempView("ratings")
    /**
      * 统计评分最多
      *
      */
    val rateMoreMiviesDF=spark.sql("select mid,count(mid) as count from ratings group by mid")
    //把结果写入到对应的mongdb表中
    storeDFInMongDB(rateMoreMiviesDF,RATE_MOVIES_COLLECTION)

    /**
      * 统计近期热门，按照yyyyMM格式选取最近的评分数据，统计评分个数
      */
      //创建一个日期格式化工具
    val simpleDateFormat =new SimpleDateFormat("yyyyMM")
    //注册udf，把时间戳转换成年月格式
    spark.udf.register("changeDate",(x: Int)=>simpleDateFormat.format(new Date(x*1000)))
    val ratingOfYearMonth=spark.sql("select mid,score,changDate(timestamp) as yearmonth from ratings")
    ratingOfYearMonth.createOrReplaceTempView("ratingOfMonth")
    //从rating中查找电影在各个月份的评分，mid，count,yearmonth
    val rateMoreRecentlyMoviesDF=spark.sql("select mid,count(mid) as count,yearmonth from ratingOfMonth group by yearmonth,mid order  by yearmonth")
    //存入mongodb
    storeDFInMongDB(rateMoreRecentlyMoviesDF,RATE_MORE_RECENTLY_MOVIES)

    /**
      *
      * 优质电影统计，统计电影的评分
      */
    val averageMoiesDF=spark.sql("select mid,avg(score) as avg from rating greoup by mid")
    storeDFInMongDB(averageMoiesDF,AVERAGE_MOVIES)

    /**
      *
      * 各个类别电影top统计
      *
      */
      //定义所有类别
    val genres=List("Action","Adventure","Animation","Comedy","Crima","Documentary","Drama")
    //把平均评分加入movie表里，加一列
    val movieWithScore =movieDF.join(averageMoiesDF,"mid")
    //做笛卡尔积，把genres转成rdd
    val genresRDD=spark.sparkContext.makeRDD(genres)
    //计算类别top10，首先对类别和电影做笛卡尔积
    val genresTopMoviesDF =genresRDD.cartesian(movieWithScore.rdd)
        .filter{
          //找出movie的字段genres值包含当前类别的哪些
          case (genres,row) => row.getAs[String]("genres").toLowerCase.contains(genres.toLowerCase)
        }
        .map{
          case (genres,movieRow) => (genres,(movieRow.getAs[Int]("mid"),movieRow.getAs[Double]("score")))
        }
        .groupByKey()
        .map{
          case (genres,items) => GenersRecommendation(genres,items.toList.sortWith(_._2>_._2).take(10).map(item=>
          Recommendation(item._1,item._2)))
        }
        .toDF()
    storeDFInMongDB(genresTopMoviesDF,GENRES_TOP_MOVIES)

    spark.stop()
    }

  def storeDFInMongDB(df: DataFrame, collection_name: String)(implicit mongoConfig:MongConfig): Unit = {
  df.write
      .option("uri",mongoConfig.uri)
      .option("collection",collection_name)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

  }

}
