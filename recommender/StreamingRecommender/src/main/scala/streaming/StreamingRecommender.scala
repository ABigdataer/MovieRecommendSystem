package streaming

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, sql}
import redis.clients.jedis.Jedis
//将java客户端API转成Scala可用对象
import scala.collection.JavaConversions._


/**
  * MongoDB的连接配置
  *
  * @param uri MongoDB的连接
  * @param db  MongoDB要操作的数据库
  */
//创建case class(MongoConfig)
case class MongoConfig(val uri: String, val db: String)

//矩阵内Seq电影数据结构（rid:mid ; r:score）
case class Recommendation(rid: Int, r: Double)

//离线电影相似度矩阵
case class MovieRecs(mid: Int, recs: Seq[Recommendation])


/**
  * 1、Serializable接口是启用其序列化功能的接口。实现java.io.Serializable 接口的类是可序列化的。没有实现此接口的类将不能使它们的任意状态被序列化或逆序列化。
  * 2、获取Redis和MongoDB的连接客户端
  */
object ConnHelper extends Serializable {
  lazy val jedis = new Jedis("mini5")
  lazy val mongodbClient = MongoClient(MongoClientURI("mongodb://mini5:27017/recommender"))
}

/**
  * （1）用户u 对电影p 进行了评分，触发了实时推荐的一次计算；
  * （2）选出电影p 最相似的K 个电影作为集合S；
  * （3）获取用户u 最近时间内的K 条评分，包含本次评分，作为集合RK；
  * （4）计算电影的推荐优先级，产生<qID,>集合updated_S；
  *  (5)将updated_S 与上次对用户u 的推荐结果Rec 利用公式进行合并，产生新的推荐结果NewRec；作为最终输出。
  */
object StreamingRecommender {

  //获取的用户评分数据个数
  val MAX_USER_RATINGS_NUM = 20
  //获取当前电影的相似的电影个数
  val MAX_SIM_MOVIES_NUM = 20

  //入口方法
  def main(args: Array[String]): Unit = {

    //将参数统一封装到Map集合
    val config = Map(
      "spark.cores" -> "local[*]", //Spark运行线程数和主节点
      "mongo.uri" -> "mongodb://mini5:27017/recommender", //MongoDB数据库的访问路径
      "mongo.db" -> "recommender", //MongoDB数据库的表名
      "kafka.topic" -> "recommender" //Kafka监听的队列
    )

    /**
      * 1、创建一个SparkConf配置
      * ①设置APPName
      * ②设置项目运行的主节点和线程数
      */
    val sparkconf = new SparkConf().setAppName("StreamingRecommender").setMaster(config("spark.cores"))

    //创建一个sparkSession
    val spark = new sql.SparkSession.Builder().config(sparkconf).getOrCreate()

    //StreamingContext会在底层创建出SparkContext,用来处理数据
    val sc = spark.sparkContext
    //创建StreamingContext，且指定每隔2秒处理一次新数据
    val ssc = new StreamingContext(sc, Seconds(2))

    //导入隐式转换，如果不到人无法将RDD转换成DataFrame
    import spark.implicits._

    /**
      * 广播电影相似度矩阵
       */
    val simMoviesMatrix = spark
      .read
      .option("uri",config("mongo.uri"))
      //获取电影相似度矩阵的数据
      .option("collection", "MovieRecs")
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRecs]
      .rdd
      //封装成固定格式（Map[Int, Map[Int,Double]]）
      .map{recs =>
        (recs.mid,recs.recs.map(x => (x.rid,x.r)).toMap)
      }
      //返回hashMap包含所有RDD中的分片，key如果重复，后边的元素会覆盖前面的元素。
      .collectAsMap()

    //广播变量（让程序向所有工作节点发送数据，以供一个或多个SparkSession操作使用）
    val simMoviesMatrixBroadCast = sc.broadcast(simMoviesMatrix)

    //随便写的一个小程序，触发广播变量的调用，使后面再次使用之前数据就已经转换成功，加快代码执行速度
    val abc = sc.makeRDD(1 to 2)
    abc.map(x=> simMoviesMatrixBroadCast.value.get(1)).count()

    /**
      * 1、 声明MongoDB的参数配置
      * 2、 "mongo.uri" -> "mongodb://mini5:27017/recommender"
      * "mongo.db" -> "recommender"
      */
    implicit val mongoConfig = MongoConfig(config.get("mongo.uri").get, config.get("mongo.db").get)

    //创建Kafka的链接
    val kafkaPara = Map(
      "bootstrap.servers" -> "mini5:9092", //kafka地址
      "key.deserializer" -> classOf[StringDeserializer], //key的反序列化类
      "value.deserializer" -> classOf[StringDeserializer], //value的反序列化类
      "group.id" -> "recommender", //消费者组
      "auto.offset.reset" -> "latest" //消费的位置
    )

    //Flume采集服务器上后台产生的日志文件，经由KafkaStream组件梳理后输出数据（格式为 UID/MID/SCORE/TIMESTAMP）到Kafka
    val kafkaStream = KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](Array(config("kafka.topic")), kafkaPara))

    //产生评分流 数据格式为 UID/MID/SCORE/TIMESTAMP 转换为 （uid,mid,score,timestamp）的RDD
    val ratingStream = kafkaStream.map {
      case msg =>
        var attr = msg.value().split("\\|")
        (attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
    }

    ratingStream.foreachRDD
    { rdd =>
      rdd.map {
        case (uid, mid, score, timestamp) =>
          println("<><><><><><><><><>")

          //从Redis中获取当前最近的M次电影评分
          val userRecentlyRatings = getUserRecentlyRating(MAX_USER_RATINGS_NUM,uid,ConnHelper.jedis)

          //获取电影P最相似的K个电影
          val simMovies = getTopSimMovies(MAX_SIM_MOVIES_NUM,mid,uid,simMoviesMatrixBroadCast.value)

          //计算待选电影的推荐优先级
          val streamRecs = computeMovieScores(simMoviesMatrixBroadCast.value,userRecentlyRatings,simMovies)

          //将数据保存到MongoDB
          saveRecsToMongoDB(uid,streamRecs)
      }
      //action算子触发程序运行
      .count()
    }

    //启动Streaming程序
    ssc.start()
    ssc.awaitTermination()
  }



  /**
    * 从Redis中获取当前最近的num次电影评分
    *
    * @param num
    * @param uid
    * @param jedis
    * @return Array[(mid, score)]
    */
  def getUserRecentlyRating(num: Int, uid: Int, jedis: Jedis): Array[(Int, Double)] = {
    //当用户对一个电影进行打分后，将会触发数据存储到Redis
    //(jedis.lpush("uid:" + rating.getUid(), rating.getMid() + ":" + rating.getScore());

    //从用户的队列中取出num个评论
    jedis.lrange("uid:" + uid.toString, 0, num)
      //import scala.collection.JavaConversions._(将java客户端API转成Scala可用对象)
      .map
    {
      item =>
      val attr = item.split("\\:")
      (attr(0).trim.toInt, attr(1).trim.toDouble)
    }.toArray
  }

  /**
    * 获取当前电影K个相似的电影
    *
    * @param num        相似电影的数量
    * @param mid        当前电影的ID
    * @param uid        当前的评分用户
    * @param simMovies  电影相似度矩阵的广播变量值
    * @param mongConfig MongoDB的配置
    * @return Array[Int] 相似电影的mid数组集合
    */
  def getTopSimMovies(num: Int, mid: Int, uid: Int, simMovies: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]])(implicit mongConfig: MongoConfig): Array[Int] = {
    //从广播变量的电影相似度矩阵中获取电影mid所有的相似电影
    val allSimMovies = simMovies.get(mid).get.toArray
    //获取用户已经观看过得电影ID
    val ratingExist = ConnHelper.mongodbClient(mongConfig.db)("Rating")
      .find(MongoDBObject("uid" -> uid))
      .toArray
      .map { item =>
        item.get("mid").toString.toInt
      }
    //过滤掉已经评分过得电影ID，并根据相似度排序并输出未看过的num个电影ID
    allSimMovies.filter(x => !ratingExist.contains(x._1))
      .sortWith(_._2 > _._2)
      .take(num)
      .map(x => x._1)
  }

  /**
    * 计算待选电影的推荐优先级
    * @param simMovies            电影相似度矩阵
    * @param userRecentlyRatings  用户最近的k次评分
    * @param topSimMovies         当前电影最相似的K个电影
    * @return
    */
  def computeMovieScores(simMovies:scala.collection.Map[Int,scala.collection.immutable.Map[Int,Double]],userRecentlyRatings:Array[(Int,Double)],topSimMovies: Array[Int]): Array[(Int,Double)] ={

    //用于保存每一个待选电影和最近评分的每一个电影的权重得分
    val score = scala.collection.mutable.ArrayBuffer[(Int,Double)]()

    //用于保存每一个电影的增强因子数
    val increMap = scala.collection.mutable.HashMap[Int,Int]()

    //用于保存每一个电影的减弱因子数
    val decreMap = scala.collection.mutable.HashMap[Int,Int]()

    for (topSimMovie <- topSimMovies; userRecentlyRating <- userRecentlyRatings){
      val simScore = getMoviesSimScore(simMovies,userRecentlyRating._1,topSimMovie)
      if(simScore > 0.6){
        score += ((topSimMovie, simScore * userRecentlyRating._2 ))
        if(userRecentlyRating._2 > 3){
          increMap(topSimMovie) = increMap.getOrDefault(topSimMovie,0) + 1
        }else{
          decreMap(topSimMovie) = decreMap.getOrDefault(topSimMovie,0) + 1
        }
      }
    }

    score.groupBy(_._1).map{case (mid,sims) =>
      (mid,sims.map(_._2).sum / sims.length + log(increMap(mid)) - log(decreMap(mid)))
    }.toArray

  }

  //取2的对数
  def log(m:Int):Double ={
    math.log(m) / math.log(2)
  }

  /**
    * 获取当前电影之间的相似度
    * @param simMovies       电影相似度矩阵
    * @param userRatingMovie 用户已经评分的电影
    * @param topSimMovie     候选电影
    * @return
    */
  def getMoviesSimScore(simMovies:scala.collection.Map[Int,scala.collection.immutable.Map[Int,Double]], userRatingMovie:Int, topSimMovie:Int): Double ={
    simMovies.get(topSimMovie) match {
      case Some(sim) => sim.get(userRatingMovie) match {
        case Some(score) => score
        case None => 0.0
      }
      case None => 0.0
    }
  }

  /**
    * 将数据保存到MongoDB    uid -> 1,  recs -> 22:4.5|45:3.8
    * @param streamRecs  流式的推荐结果
    * @param mongConfig  MongoDB的配置
    */
  def saveRecsToMongoDB(uid:Int,streamRecs:Array[(Int,Double)])(implicit mongConfig: MongoConfig): Unit ={

    //到StreamRecs的连接
    val streaRecsCollection = ConnHelper.mongodbClient(mongConfig.db)("StreamRecs")

    streaRecsCollection.findAndRemove(MongoDBObject("uid" -> uid))

    streaRecsCollection.insert(MongoDBObject("uid" -> uid, "recs" -> streamRecs.map(x=> x._1+":"+x._2).mkString("|")))

  }

}
