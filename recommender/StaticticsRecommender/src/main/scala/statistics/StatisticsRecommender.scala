package statistics

import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.{SparkConf, sql}

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
  * 保存到MongoDB的Movie数据集，数据集字段通过^分割
  *
  * 151^                          电影的ID
  * Rob Roy (1995)^               电影的名称
  * In the highlands ....^        电影的描述
  * 139 minutes^                  电影的时长
  * August 26, 1997^              电影的发行日期
  * 1995^                         电影的拍摄日期
  * English ^                     电影的语言
  * Action|Drama|Romance|War ^    电影的类型
  * Liam Neeson|Jessica Lange...  电影的演员
  * Michael Caton-Jones           电影的导演
  *
  */
//创建case class（Movie）
case class Movie(val mid: Int, val name: String, val descri: String, val timelong: String, val issue: String,
                 val shoot: String, val language: String, val genres: String, val sctors: String, val directors: String)

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

/**
  * 电影类别的推荐
  *
  * @param genres 电影的类别
  * @param recs   top10的电影的集合
  */
case class GenresRecommendation(genres: String, recs: Seq[Recommendation])

/**
  * 一、离线统计算法模块功能分析
  * 1、优质电影
  *       获取所有历史数据中评分最多的电影的集合，统计每个电影的评分数  -》  RateMoreMovies
  * 2、热门电影
  *      按照月来统计，这个月中评分数量最多的电影我们认为是热门电影，统计每个月中的每个电影的评分数量   -》  RateMoreRecentlyMovie
  * 3、统计电影的平均评分
  *      将Rating数据集中所有的用户评分数据进行平均，计算每一个电影的平均评分  -》 AverageMovies
  * 4、统计每种类别电影的TOP10电影
  *      将每种类别的电影中评分最高的10个电影分别计算出来，  -》  GenresTopMovies
  */
object StatisticsRecommender {

  //程序入口方法
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
    val sparkconf = new SparkConf().setAppName("StatisticsRecommender").setMaster(config.get("spark.cores").get)

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

    /**
      * 1、加载数据:
      * ①评分数据
      * ②电影相关数据
      */
    val ratingDF = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", "Rating")
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRating]
      .toDF()

    val movieDF = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", "Movie")
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movie]
      .toDF()

    //创建(注册)一张名叫ratings的表
    ratingDF.createOrReplaceTempView("ratings")

    /**
      * 1 、统计优质电影 所有历史数据中评分最多的电影的集合
      * 2 、数据结构 : mid,count
      * 3 、通过Rating数据集，用mid进行group by操作，count计算总数
      */
    val rateMoreMoviesDF = spark.sql("select mid, count(mid) as count from ratings group by mid")

    //将统计后数据写入到MongoDB的对应表中
    rateMoreMoviesDF.write
      //指定URI
      .option("uri", mongoConfig.uri)
      //指定操作的数据库表
      .option("collection", "RateMoreMovies")
      //指定是复写还是追加数据
      .mode("overwrite")
      //指定用哪个驱动写
      .format("com.mongodb.spark.sql")
      .save()

    /**
      * 1 、统计热门电影 每个月中的每个电影的评分数量
      * 2 、数据结构 ： mid,count,time
      * 3 、需要注册一个UDF函数，用于将Timestamp这种格式的数据转换成  yyyyMM  这个格式，simpleDateFormat
      * 4 、需要将RatingDF装换成新的RatingOfMouthDF【只有日期数据发生了转换】
      * 5 、通过group by 年月，mid 来完成统计
      */
    //日期格式化工具
    val simpleDateFormat = new SimpleDateFormat("yyyyMM")

    //注册一个UDF函数，用于将timestamp转换成年月格式 : 1260759144000  => 200402
    spark.udf.register("changeDate", (x: Int) => simpleDateFormat.format(new Date(x * 1000L)).toInt)

    //将原来的Rating数据集中的时间转换成年月格式
    val ratingOfYeahMouth = spark.sql("select mid , score , changeDate(timestamp) as yeahmouth from ratings ")

    //将新的数据集注册成一张表
    ratingOfYeahMouth.createOrReplaceTempView("ratingOfMouth")

    //按月查询不同电影的评分数（返回DF）
    val rateMoreRecentlyMovies = spark.sql("select mid , count(mid) as count , yeahmouth from ratingOfMouth group by yeahmouth,mid")

    //将统计后数据写入到MongoDB的对应表中
    rateMoreRecentlyMovies.write
      //指定URI
      .option("uri", mongoConfig.uri)
      //指定操作的数据库表
      .option("collection", "RateMoreRecentlyMovies")
      //指定是复写还是追加数据
      .mode("overwrite")
      //指定用哪个驱动写
      .format("com.mongodb.spark.sql")
      .save()

    /**
      * 1 、 统计电影的平均评分
      * 2 、 数据结构 ：mid,avg
      * 3 、 通过Rating数据集，用户mid进行group by操作，avg计算评分
      */
    val averageMoviesDF = spark.sql("select mid , avg(score) as avg from ratings group by mid")
    //将统计后数据写入到MongoDB的对应表中
    averageMoviesDF.write
      //指定URI
      .option("uri", mongoConfig.uri)
      //指定操作的数据库表
      .option("collection", "AverageMovies")
      //指定是复写还是追加数据
      .mode("overwrite")
      //指定用哪个驱动写
      .format("com.mongodb.spark.sql")
      .save()

    /**
      * 1 、 统计每种类别电影的TOP10
      * 2 、 需要通过JOIN操作将电影的平均评分数据和Movie数据集进行合并，产生MovieWithScore数据集
      * 3 、 需要将电影的类别数据转换成RDD，GenresRDD
      * 4 、 将GenresRDD和MovieWithScore数据集进行笛卡尔积，产生一个 N * M行的数据集。
      * 5 、 通过过滤操作，过滤掉电影的真实类别和GenresRDD中的类别不匹配的电影
      * 6 、 通过Genres作为Key，进行groupByKey操作，将相同电影类别的电影进行聚集。
      * 7 、 通过排序和提取，获取评分最高的10个电影
      * 8 、 将结果输出到MongoDB中
      * 9 、 2、3、4过程可以通过spark的函数  explode  来替代完成
      */
    //需要用left join，因为只需要有评分的电影数据集
    val movieWithScore = movieDF.join(averageMoviesDF, Seq("mid", "mid"))

    //所有的电影类别
    val genres = List("Action", "Adventure", "Animation", "Comedy", "Ccrime", "Documentary",
      "Drama", "Family", "Fantasy", "Foreign", "History", "Horror", "Music", "Mystery", "Romance",
      "Science", "Tv", "Thriller", "War", "Western")

    //将电影类别转换成RDD
    val genresRDD = spark.sparkContext.makeRDD(genres)

    //计算电影类别Top10
    //将电影类别和电影数据进行笛卡儿积操作，得出每种类别电影和每条电影数据的集合
    val genrenTopTenMovies = genresRDD.cartesian(movieWithScore.rdd)
      .filter {
        //过滤掉电影类型不匹配的电影数据(全部转换成小写)
        case (genres, row) => row.getAs[String]("genres").toLowerCase.contains(genres.toLowerCase)
      }
      .map {
        //取出电影ID和平均评分，生成RDD[genres,Iter[mid,avg]]
        case (genres, row) => {
          (genres, (row.getAs[Int]("mid"), row.getAs[Double]("avg")))
        }
      }
      //按照电影类别进行分组
      .groupByKey()
      .map
    {
      case (genres,items)
      =>
        GenresRecommendation
        (
          genres,
          items
            .toList
            .sortWith( _._2 > _._2 )//排序
            .take(10)
            .map(
              item
              =>
                Recommendation(item._1,item._2)
            )
        )
    }
      .toDF()

    //将数据输出到MongoDB
    genrenTopTenMovies.write
      //指定URI
      .option("uri", mongoConfig.uri)
      //指定操作的数据库表
      .option("collection", "GenresTopMovies")
      //指定是复写还是追加数据
      .mode("overwrite")
      //指定用哪个驱动写
      .format("com.mongodb.spark.sql")
      .save()

    //关闭Spark
    spark.close()

  }

}
