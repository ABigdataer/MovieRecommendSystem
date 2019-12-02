package offline

import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, sql}
import org.jblas.DoubleMatrix

/**
  * Rating数据集，用户对于电影的评分数据集，用，分割
  *
  * 1,           用户的ID
  * 31,          电影的ID
  * 2.5,         用户对于电影的评分
  * 1260759144   用户对于电影评分的时间
  */
//创建case class(Rating)
//case class MovieRating(val uid: Int, val mid: Int, val score: Double, val timestamp: Int)

/**
  * MongoDB的连接配置
  *
  * @param uri MongoDB的连接
  * @param db  MongoDB要操作的数据库
  */
//创建case class(MongoConfig)
//case class MongoConfig(val uri: String, val db: String)

//矩阵内Seq电影数据结构（rid:mid ; r:score）
case class Recommendation(rid: Int, r: Double)

//离线用户推荐电影矩阵
case class UserRecs(uid: Int, recs: Seq[Recommendation])

//离线电影相似度矩阵
case class MovieRecs(mid: Int, recs: Seq[Recommendation])

//创建case class（Movie）
case class Movie(val mid: Int, val name: String, val descri: String, val timelong: String, val issue: String,
                 val shoot: String, val language: String, val genres: String, val sctors: String, val directors: String)


/**
  * ALS离线推荐算法模块
  * 1、训练ALS推荐模型
  * 2、计算用户推荐矩阵
  * 3、计算电影相似度矩阵
  */
object OfflineRecommender {

  //入口方法
  def main(args: Array[String]): Unit = {

    //将参数统一封装到Map集合
    val config = Map(
      "spark.cores" -> "local[*]", //Spark运行线程数和主节点
      "mongo.uri" -> "mongodb://mini5:27017/recommender", //MongoDB数据库的访问路径
      "mongo.db" -> "recommender" //MongoDB数据库的表名
    )

    /**
      * 1、创建一个SparkConf配置
      * ①设置APPName
      * ②设置项目运行的主节点和线程数
      */
    val sparkconf = new SparkConf().setAppName("OfflineRecommender").setMaster(config.get("spark.cores").get)
      //每个执行器（worker）进程分配的内存（任务执行节点）
      .set("spark.executor.memory", "6G")
      //每个驱动器(SparkSubmit Driver)进程分配的内存(向Master提交任务、申请资源)
      .set("spark.driver.memory", "2G")

    //创建一个sparkSession
    val spark = new sql.SparkSession.Builder().config(sparkconf).getOrCreate()

    /**
      * 1、 声明MongoDB的参数配置
      * 2、 "mongo.uri" -> "mongodb://mini5:27017/recommender"
      * "mongo.db" -> "recommender"
      */
    val mongoConfig = MongoConfig(config.get("mongo.uri").get, config.get("mongo.db").get)

    //导入隐式转换，如果不到人无法将RDD转换成DataFrame
    import spark.implicits._

    //读取MongoDB中的业务数据

    //取出电影标签数据并转成RDD ===》数据格式为 (uid,mid,score)
    val ratingRDD = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", "Rating")
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRating]
      .rdd
      .map(rating => (rating.uid, rating.mid, rating.score))
      //此处需要缓存一下数据，避免多次迭代应用（重复计算），造成内存溢出错误
      .cache()

    //取出用户ID数据集 RDD[Int]（distinct去重）
    val userRDD = ratingRDD.map(_._1).distinct()
    //取出电影ID数据集 RDD[Int]
    val movieRDD = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", "Movie")
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movie]
      .rdd
      .map(_.mid)
      //此处需要缓存一下数据，避免多次迭代应用（重复计算），否则会造成内存溢出错误
      .cache()

    /**
      * 训练ALS模型
      * 1、需要构建RDD[Rating]类型的训练数据
      * 2、直接通过ALS.train方法来进行训练
      */
    //创建训练数据集
    val trainData = ratingRDD.map(x => Rating(x._1, x._2, x._3))

    //rank：训练的数据集有多少特征（ALS优化-损失函数） iterations:训练的次数 lambda:（ALS优化--损失函数的吉洪诺夫正则化参数_精度）
    val (rank, iterations, lambda) = (50, 5, 0.01)

    //训练ALS模型
    val model = ALS.train(trainData, rank, iterations, lambda)

    /**
      * 计算用户推荐矩阵
      * 1、生成userMovieRDD  RDD[(Int,Int)]
      * 2、通过ALS模型的predict方法来预测用户将对电影的评分
      * 3、将数据通过GroupBy处理后排序，取前N个作为推荐结果
      */

    //求用户ID和电影ID的笛卡尔积 ALS的predict方法需要传进参数RDD[(Int,Int)] （两个int分别为用户id和电影id）
    val userMovies = userRDD.cartesian(movieRDD)
    val preRatings: RDD[Rating] = model.predict(userMovies)

    /** 1、将预测的评分转换成key,value格式，以便使用groupByKey进行分组
      * 2、 ①rating.user为用户ID
      *    ②rating.product电影ID
      *    ③rating.rating为预测评分
      */
    val userRecs = preRatings
      .filter(_.rating > 0)//过滤掉评分小于零的数据
      .map(rating => (rating.user, (rating.product, rating.rating)))
      .groupByKey()//通过用户ID进行分组
      .map {
        case (uid, recs)
        =>
          UserRecs(
            uid,
            recs.toList.sortWith(_._2 > _._2)//根据预测的电影评分排序
          .take(20)//只取前面20个电影作为推荐数据
          .map(x => Recommendation(x._1, x._2))
          )
      }.toDF()

    //将计算后用户电影推荐数据写入到MongoDB的对应表中
    userRecs.write
      //指定URI
      .option("uri", mongoConfig.uri)
      //指定操作的数据库表
      .option("collection", "UserRecs")
      //指定是复写还是追加数据
      .mode("overwrite")
      //指定用哪个驱动写
      .format("com.mongodb.spark.sql")
      .save()

    /**
      * 计算电影相似度矩阵
      * 1、获取电影的特征矩阵，取出训练的模型的product（电影ID）和Features（特征，实际取什么特征为算法内部数据，并不知道具体是什么特征），将Features转换成DoubleMatrix
      * 2、电影的特征矩阵之间做笛卡尔积，通过余弦相似度计算两个电影的相似度
      * 3、将数据通过GroupBy处理后，输出
      */
    //获取电影的特征矩阵
    val moviesFeatures = model.productFeatures.map{
      case(mid,freatures) => (mid,new DoubleMatrix(freatures))
    }

    //电影的特征矩阵之间做笛卡尔积,过滤掉mid相等的数据
    val movieRecs = moviesFeatures.cartesian(moviesFeatures)
        .filter
        {
          case (a,b) => a._1 != b._1
        }
      //遍历两个电影的特征，由consinSim方法求出两个电影特征之间的相似度评分
      .map
      {
        case (a,b) =>
        val simScore = this.consinSim(a._2,b._2)
          //返回电影a的相似度矩阵 （mid,[mid,simScore]）
        (a._1,(b._1,simScore))
      }
      //过滤掉相似度大于0.6的数据（本相似度求得是两个余弦相似度，当夹角越大，则相似度越低）
      .filter(_._2._2 > 0.6)
      //根据电影的mid进行分组
      .groupByKey()
      //将数据转换成统一格式处理 MovieRecs（mid,（[mid,simScore]，[mid,simScore]，[mid,simScore]。。。））
      .map
      {
      case (mid,items) =>
      MovieRecs(mid,items.toList.map(x => Recommendation(x._1,x._2)))
      }.toDF()

    movieRecs
      .write
      .option("uri", mongoConfig.uri)
      .option("collection", "MovieRecs")
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
    //关闭spark
    spark.close()

  }

  //计算两个电影之间的余弦相似度
  def consinSim(movie1: DoubleMatrix, movie2:DoubleMatrix) : Double ={
    movie1.dot(movie2) / ( movie1.norm2()  * movie2.norm2() )
  }

}
