package com

import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
case class MongoConfig(uri:String,db:String)
object ALSTrainer {

  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_TAG_COLLECTION = "Tag"
  val MOVIE_RECS = "MovieRecs"
  val USER_MAX_RECOMMENDATION = 20
  val USER_RECS = "USERRECS"


  def main(args: Array[String]): Unit = {
    val config =Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://",
      "mongo.db" -> "recommender"
    )
    val sparkConf=new SparkConf().setMaster(config("spark.cores")).setAppName("OffineRecommender")
    //创建一个SparkSession
    val spark=SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._
    implicit  val mongoConfig=MongoConfig(config("mongo.uri"),config("monggo.db"))
    //加载评分数据
    val ratingRDD=spark.read
      .option("uri", mongoConfig.uri)
      .option("collection" ,MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRating]
      .rdd
      .map(rating => Rating(rating.uid,rating.mid,rating.score))
      .cache()

    //随机切分数据集，生成训练集和测试集
    val splits =ratingRDD.randomSplit(Array(0.8,0.2))
    val trainingRDD= splits(0)
    val testRDD=splits(1)
    //模型参数选择，输出最优参数
    adjustALSParam(trainingRDD,testRDD)
    spark.close()
  }
  def adjustALSParam(trainData: RDD[Rating], testData: RDD[Rating]):Unit ={
    val result =for( rank <- Array(20,50,100);lambda <-Array(0.001,0.01,0.1))
      yield {
        val model =ALS.train(trainData,rank,50,lambda)
        val rmse =getRMSE(model,testData)
        (rank,lambda,rmse)
      }
    //控制台打印输出最优参数
   /* print(result.sortBy(_._3).head)*/
    println(result.minBy(_._3))
  }
def getRMSE(model: MatrixFactorizationModel, data: RDD[Rating]): Unit ={
  //计算预测评分
  val userProducts =data.map(item =>(item.user,item.product))
  val predictRating =model.predict(userProducts)
  //以uid，mid作为外键，innner join实际观测值和预测值
  val observed=data.map(item => ((item.user,item.product),item.rating))
  val predict =predictRating.map(item =>((item.user,item.product),item.rating))
  //内连接得到(uid,mid),(actual,predict)
  observed.join(predict).map{
    case ((uid,mid),(actual,pre)) =>
      val err =actual -pre
      err*err
  }.mean()
}
}
