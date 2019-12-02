package dataloader

import java.net.InetAddress

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.sql.DataFrame
import org.apache.spark.{SparkConf, sql}
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient
import scalamodel.Model._

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
  * Tag数据集，用户对于电影的标签数据集，用，分割
  *
  * 15,          用户的ID
  * 1955,        电影的ID
  * dentist,     标签的具体内容
  * 1193435061   用户对于电影打标签的时间
  */
//创建case class（Tag）
case class Tag(val uid: Int, val mid: Int, val tag: String, val timestamp: Int)

/**
  * MongoDB的连接配置
  *
  * @param uri MongoDB的连接
  * @param db  MongoDB要操作的数据库
  */
//创建case class(MongoConfig)
case class MongoConfig(val uri: String, val db: String)

/**
  * ElasticSesrch的连接配置
  *
  * @param httpHosts      http的主机列表，以 ，分割
  * @param transportHosts Transport的主机列表，以 ，分割
  * @param index          需要操作的索引
  * @param clustername    ES集群的名称
  */
//创建case class(ESConfig)
case class ESConfig(val httpHosts: String, val transportHosts: String, val index: String, val clustername: String)




//数据的主加载服务
object DataLoader {

  /**
    * 程序的入口
    * @param args
    */
  def main(args: Array[String]): Unit = {

  /*  if (args.length != 7)
      {
        System.err.println("参数个数不为7，请检查参数后再运行项目！！！")
        System.exit(1)
      }

    val mongo_server = args(0);//MongoDB数据库的访问路径 mongodb://mini5:27017/recommender
    val es_http_server = args(1); // mini5:9200
    val es_trans_server = args(2); //mini5:9300
    val es_cluster_name = args(3); //es-cluster

    val movie_data_path = args(4);
    val rating_data_path = args(5);
    val tag_data_path = args(6);*/

    val MOVIE_DATAPATH = "D:\\项目实战\\电影推荐系统（Spark）\\MovieRecommendSystem\\recommender\\dataloader\\src\\main\\resources\\movies.csv";
    val RATING_DATAPATH = "D:\\项目实战\\电影推荐系统（Spark）\\MovieRecommendSystem\\recommender\\dataloader\\src\\main\\resources\\ratings.csv";
    val TAG_DATAPATH = "D:\\项目实战\\电影推荐系统（Spark）\\MovieRecommendSystem\\recommender\\dataloader\\src\\main\\resources\\tags.csv";

    val config = Map(
      "spark.cores" -> "local[*]", //Spark运行线程数和主节点

      "mongo.uri" -> ("mongodb://mini5:27017/" + "recommender"),
      "mongo.db" -> "recommender", //MongoDB的数据库名

      "es.httpHosts" -> "mini5:9200",
      "es.transportHosts" -> "mini5:9300",
      "es.index" -> "recommender",
      "es.cluster.name" -> "es-cluster"
    )

    /**
      * 1、创建一个SparkConf配置
      * ①设置APPName
      * ②设置项目运行的主节点和线程数
      */
    val sparkconf = new SparkConf().setAppName("DataLoader").setMaster(config.get("spark.cores").get)

    //创建一个sparkSession
    //Spark2.*之后SparkSession整合了SQLContext和HiveContext,用于SaprkSQL提交作业
    val spark = new sql.SparkSession.Builder().config(sparkconf).getOrCreate()


    /*<==================================将数据初始化到MongoDB start=====================================================================================>*/

    //导入隐式转换，如果不导人无法将RDD转换成DataFrame
    import spark.implicits._

    //加载Movie数据集(从指定的地址创建RDD)
    val movieRDD = spark.sparkContext.textFile(MOVIE_DATAPATH)

    /**
      * 1、创建case class
      * 2、将RDD和case class关联
      * 3、导入隐式转换
      * 4、将MRDD转换为DataFrame
      */
    val movieDF = movieRDD.map(item => {
      val attr = item.split("\\^")
      //trim去除空格
      Movie(attr(0).toInt, attr(1).trim, attr(2).trim, attr(3).trim, attr(4).trim, attr(5).trim, attr(6).trim, attr(7).trim, attr(8).trim, attr(9).trim)
    }
    ).toDF()

    //加载Rating数据集(从指定的地址创建RDD)
    val ratingRDD = spark.sparkContext.textFile(RATING_DATAPATH)
    //将RatingRDD转换为DataFrame
    val ratingDF = ratingRDD.map(item => {
      val attr = item.split(",")
      MovieRating(attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
    }
    ).toDF()

    //加载Tag数据集(从指定的地址创建RDD)
    val tagRDD = spark.sparkContext.textFile(TAG_DATAPATH)
    //将TagRDD转换为DataFrame
    val tagDF = tagRDD.map(item => {
      val attr = item.split(",")
      Tag(attr(0).toInt, attr(1).toInt, attr(2).trim, attr(3).toInt)
    }).toDF()

    /**
      * 1、 将mongoConfig配置参数映射到 case class,并作为隐式参数传递
      * 2、  "mongo.uri" -> "mongodb://mini5:27017/recommender"
      *      "mongo.db" -> "recommender"
      */
    implicit val mongoConfig = MongoConfig(config.get("mongo.uri").get, config.get("mongo.db").get)

    //将数据初始化到MongoDB中
    storeDataInMongoDB(movieDF, ratingDF, tagDF)(implicitly)


    /*<==================================将数据初始化到MongoDB end=====================================================================================>*/


    /*<==================================将数据初始化到ES start=====================================================================================>*/

    //导入SparkSQL的函数集
    import org.apache.spark.sql.functions._

    //先将Tag数据集进行处理，处理后形式为MID,tag1|tag2|...(行转列、聚合操作，参考Hive行转列知识点)
    val newTag = tagDF.groupBy($"mid")
      .agg(concat_ws("|", collect_set($"tag"))
        .as("tags"))
      .select("mid", "tags")

    //将聚合后Tag数据集和Movie数据JOIN，产生新的Movie数据集（去除没有标签数据的电影）
    val movieWithTagsDF = movieDF.join(newTag, Seq("mid", "mid"), "left")

    /** 1、将ESConfig配置参数映射到 case class,并作为隐式参数传递
      * 2、 "es.httpHosts" -> "mini5:9200",
      * "es.transportHosts" -> "mini5:9300",
      * "es.index" -> "recommender",
      * "es.cluster.name" -> "es-cluster"
      */
    implicit val esConfig = ESConfig(config.get("es.httpHosts").get, config.get("es.transportHosts").get, config.get("es.index").get, config.get("es.cluster.name").get)

    //将新的数据集初始化到ES中
    storeDataInES(movieWithTagsDF)(implicitly)

    /*<==================================将数据初始化到ES end=====================================================================================>*/

    //关闭Spark
    spark.stop()

  }

  //将数据保存到MongoDB中的方法(柯里化，导入隐式参数)
  def storeDataInMongoDB(movieDF: DataFrame, ratingDF: DataFrame, tagDF: DataFrame)(implicit mongoConfig: MongoConfig): Unit = {

    //新建一个到MongoDB的连接
    val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))

    //如果MongoDB中有对应的表（Collection），那么应该删除（mongoClient有applay方法，详见源代码）
    mongoClient(mongoConfig.db)("Movie").dropCollection()
    mongoClient(mongoConfig.db)("Rating").dropCollection()
    mongoClient(mongoConfig.db)("Tag").dropCollection()

    //将读取的所有数据写入到MongoDB的对应表中（通过Spark SQL提供的write方法进行数据的分布式插入）
    movieDF.write
      //指定URI
      .option("uri", mongoConfig.uri)
      //指定操作的数据库表
      .option("collection", "Movie")
      //指定是复写还是追加数据
      .mode("overwrite")
      //指定用哪个驱动写
      .format("com.mongodb.spark.sql")
      .save()

    ratingDF.write
      //指定URI
      .option("uri", mongoConfig.uri)
      //指定操作的数据库表
      .option("collection", "Rating")
      //指定是复写还是追加数据
      .mode("overwrite")
      //指定用哪个驱动写
      .format("com.mongodb.spark.sql")
      .save()

    tagDF.write
      //指定URI
      .option("uri", mongoConfig.uri)
      //指定操作的数据库表
      .option("collection", "Tag")
      //指定是复写还是追加数据
      .mode("overwrite")
      //指定用哪个驱动写
      .format("com.mongodb.spark.sql")
      .save()

    /**
      * 1、对数据表建立索引
      * 2、插入的文档对象会默认添加_id属性，这个属性对应一个唯一的id,是文档的唯一标识（可手动指定，但需要确保唯一性，不建议手动指定）
      */
    mongoClient(mongoConfig.db)("Movie").createIndex(MongoDBObject("mid" -> 1))
    mongoClient(mongoConfig.db)("Rating").createIndex(MongoDBObject("uid" -> 1))
    mongoClient(mongoConfig.db)("Rating").createIndex(MongoDBObject("mid" -> 1))
    mongoClient(mongoConfig.db)("Tag").createIndex(MongoDBObject("uid" -> 1))
    mongoClient(mongoConfig.db)("Tag").createIndex(MongoDBObject("mid" -> 1))

    //关闭MongoDB的连接
    mongoClient.close()

  }

  //将数据保存到ES的方法
  def storeDataInES(movieWithTagDF: DataFrame)(implicit esConfig: ESConfig): Unit = {

    //新建一个配置（设置连接的集群名称）
    val settings: Settings = Settings.builder().put("cluster.name", esConfig.clustername).build()

    //新建ES的客户端（连接集群）
    val esClient = new PreBuiltTransportClient(settings)

    //需要将TransportHosts添加到esClient（假如ES是一个集群，参数es.transportHosts可能会以 ，隔开传输多个参数，所以需要进行分割处理）
    val REGEX_HOST_PORT = "(.+):(\\d+)".r //正则表达式
    esConfig.transportHosts.split(",").foreach {
      case REGEX_HOST_PORT(host: String, port: String) => {
        esClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port.toInt))
      }
    }

    //清除ES中遗留的数据并新建Index
    if (esClient.admin().indices().exists(new IndicesExistsRequest(esConfig.index)).actionGet().isExists) {
      esClient.admin().indices().delete(new DeleteIndexRequest(esConfig.index))
    }
    esClient.admin().indices().create(new CreateIndexRequest(esConfig.index))

    //将数据写入到ES
    movieWithTagDF.write
      .option("es.nodes", esConfig.httpHosts)
      .option("es.http.timeout", "100m")
      .option("es.mapping.id", "mid")
      .mode("overwrite")
      .format("org.elasticsearch.spark.sql")
      .save(esConfig.index + "/" + Movie)

  }

}
