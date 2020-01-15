
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.SparkSession
import org.jblas.DoubleMatrix
import org.apache.spark.mllib.linalg.Vector
case class MongConfig(uri:String,db:String)
case class Rating(uid:Int,mid:Int,score:Double,timestamp:Int)
case class Movie(mid:Int,name:String,descri:String,timelong:String,issue:String,shoot:String,language:String,genres:String,actors:String,directors:String)
case class Recommendation(mid:Int,score:Double)
object ContentRecommender {
//定义表名和常量
  val MONGODB_MOVIE_COLLECTION="Movie"
  val CONTENT_MOVIE_RECS="ContentMovieRecs"

  def main(args: Array[String]): Unit = {
    val config =Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )
    //创建一个sparkconf11111
    val sparkConf =new SparkConf().setMaster(config("spark.cores")).setAppName("DataLoader")
    //创建一个SparkSession
    val spark=SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._
    implicit val mongConfig=MongConfig(config("mongo.uri"),config("mongo.db"))
    //从mongodb加载数据
    val movieTagsDF =spark.read
      .option("uri",mongConfig.uri)
      .option("collection",MONGODB_MOVIE_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movie]
      .map(
        //提取mid,name,genres三项作为原始内容特征，分词器默认按照空格做分词
        x=>(x.mid,x.name,x.genres.map(c=>if(c=='|') ' ' else c))
      ).toDF("mid","name","genres")
      .cache()
    //用 TF-IDF从内容信息提取电影特征
    //创建一个分词器对原始数据做转换，生成新的一列words
    val tokenizer =new Tokenizer().setInputCol("genres").setOutputCol("words")
    //用分词器对原始数据做转换，生成新的一列
    val wordsData =tokenizer.transform(movieTagsDF)
    //引入HashingIF工具，可以把一个词语序列转换成对应的词频
    val hashingTF =new HashingTF().setInputCol("words").setOutputCol("rawfeatures").setNumFeatures(50)
    val featurizedData =hashingTF.transform(wordsData)
    //引入IDF工具，可以得到idf模型
    val idf =new IDF().setInputCol("rawFeatures").setOutputCol("features")
    //训练idf模型，得到每个词的逆文档频率
    val idfModel =idf.fit(featurizedData)
    //用模型对原始数据进行处理，得到文档中每个词的 tf-idf，作为新的特征向量
    val rescaleData=idfModel.transform(featurizedData)
    //featurizedData.show(truncate = false)
    val movieFeature =rescaleData.map(
    row => (row.getAs[Int]("mid"), row.getAs[SparseVector]("features").toArray)

    ).rdd
        .map(
          x => (x._1,new DoubleMatrix(x._2))
        )
    movieFeature.collect().foreach(println)
    wordsData.show()
  }
}
