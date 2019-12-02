package offline

import breeze.numerics.sqrt
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Rating数据集，用户对于电影的评分数据集，用，分割
  *
  * 1,           用户的ID
  * 31,          电影的ID
  * 2.5,         用户对于电影的评分
  * 1260759144   用户对于电影评分的时间
  */
//创建case class(Rating)
case class MovieRating(val uid: Int, val mid: Int, val score: Double, val timestamp: Int)

/**
  * MongoDB的连接配置
  *
  * @param uri MongoDB的连接
  * @param db  MongoDB要操作的数据库
  */
//创建case class(MongoConfig)
case class MongoConfig(val uri: String, val db: String)



object ALSTriner {

  def main(args: Array[String]): Unit = {

    //将参数统一封装到Map集合
    val config = Map(
      "spark.cores" -> "local[*]", //Spark运行线程数和主节点
      "mongo.uri" -> "mongodb://mini5:27017/recommender", //MongoDB数据库的访问路径
      "mongo.db" -> "recommender" //MongoDB数据库的表名
    )

    //创建SparkConf
    val sparkConf = new SparkConf().setAppName("ALSTrainer").setMaster(config("spark.cores"))

    //创建SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val mongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))

    import spark.implicits._

    //加载评分数据
    val ratingRDD = spark
      .read
      .option("uri",mongoConfig.uri)
      .option("collection", "Rating")
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRating]
      .rdd
      .map(rating => Rating(rating.uid,rating.mid,rating.score)).cache()

    //输出最优参数
    adjustALSParams(ratingRDD)

    //关闭Spark
    spark.close()
  }

  // 输出最终的最优参数
  def adjustALSParams(trainData:RDD[Rating]): Unit ={
    val result = for(rank <- Array(30,40,50,60,70); lambda <- Array(1, 0.1, 0.001))
      yield {
        val model = ALS.train(trainData,rank,5,lambda)
        val rmse = getRmse(model,trainData)
        (rank,lambda,rmse)
      }
    println(result.sortBy(_._3).head)
  }

  def getRmse(model:MatrixFactorizationModel, trainData:RDD[Rating]):Double={
    //需要构造一个usersProducts  RDD[(Int,Int)]
    val userMovies = trainData.map(item => (item.user,item.product))
    val predictRating = model.predict(userMovies)

    val real = trainData.map(item => ((item.user,item.product),item.rating))
    val predict = predictRating.map(item => ((item.user,item.product),item.rating))

    sqrt(
      real.join(predict).map{case ((uid,mid),(real,pre))=>
        // 真实值和预测值之间的两个差值
        val err = real - pre
        err * err
      }.mean()
    )
  }

}
