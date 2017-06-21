package feature

/**
  * Created by dengxing on 2017/4/13.
  */

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import util.{MQSender, MixtureBucketizer, MixtureQuantileDiscretizer}
import scala.collection.mutable.HashSet
import scala.collection.mutable.{ArrayBuffer, LinkedHashMap}
import scala.io.Source

object EncodeSpark_fs {

  /** 基于hql获取数据源
    *
    * @param sc
    * @param hql
    * @return 数据源的DataFrame
    */
  def getResource(sc: SparkContext, hql: String) = {
    val hiveContext = new HiveContext(sc)
    import hiveContext.sql
    val resource = sql(hql)
    resource
  }


  /** 获取特征-索引映射表
    *
    * @param sc
    * @param table
    * @param day
    * @return 特征映射表的DataFrame
    */
  def getFeatureMap(sc: SparkContext, table: String, day: String) = {
    val hiveContext = new HiveContext(sc)
    import hiveContext.sql
    val featureMap = sql("select feature,id from " + table + " where dt='" + day + "'")
    featureMap
  }


  /** 对数据源指定的连续值得列进行离散化
    *
    * @param resource
    * @param buckets
    * @return 离散化的DataFrame
    */
  def discretize1(resource: DataFrame, buckets: LinkedHashMap[(String, Int), Array[Double]]) = {
    val results_bucketed = ArrayBuffer[DataFrame]()
    val features_bucketed = ArrayBuffer[String]()

    for ((key, split) <- buckets) {
      features_bucketed += key._1
      val mixtureBucketizer = new MixtureBucketizer()
        .setBenchmark(resource.columns(0))
        .setIdentify(key._2)
        .setInputCol(key._1)
        .setOutputCol(key._1 + "_bucketed")
        .setSplits(split)
      results_bucketed += mixtureBucketizer.transform(resource)
    }
    var result = resource
    for (col <- features_bucketed) {
      result = result.drop(col)
    }
    for (t <- results_bucketed) {
      result = result.join(t, resource.columns(0))
    }
    result
  }


  /** 对数据源指定的连续值得列进行离散化
    * (自动定义离散化规则)
    * @param resource
    * @param buckets
    * @return 离散化的DataFrame
    */
  def discretize2(resource: DataFrame, buckets: LinkedHashMap[(String, Int), Int]) = {
    val results_bucketed = ArrayBuffer[DataFrame]()
    val features_bucketed = ArrayBuffer[String]()

    for ((key, numBuckets) <- buckets) {
      features_bucketed += key._1
      val mixtureQuantileDiscretizer = new MixtureQuantileDiscretizer()
        .setBenchmark(resource.columns(0))
        .setIdentify(key._2)
        .setInputCol(key._1)
        .setOutputCol(key._1 + "_bucketed")
        .setSplits(numBuckets)
      results_bucketed += mixtureQuantileDiscretizer.transform(resource)
    }
    var result = resource
    for (col <- features_bucketed) {
      result = result.drop(col)
    }
    for (t <- results_bucketed) {
      result = result.join(t, resource.columns(0))
    }
    result
  }



  /** 对数据源进行one-hot编码
    *
    * @param resource
    * @param fMap 特征映射map
    * @return one-hot编码后的RDD[String]
    */
  def oneHotEncode(resource: DataFrame, fMap: Broadcast[scala.collection.Map[String, Long]], featureSelector: Broadcast[HashSet[Long]]) = {
    val rows = resource.map {

      case Row(benchmark: String, features@_*) => {

        val row = ArrayBuffer[String](benchmark)

        for (i <- 0 until features.length) {
          val result = new StringBuilder()
          if (features(i).toString.contains(",")) {
            val interFeatures = features(i).toString.split(",")
            for (j <- 0 until interFeatures.length) {
              if (!interFeatures(j).contains("-1") && fMap.value.get(interFeatures(j)) != None) {
                //特征选择
                if (featureSelector.value.contains(fMap.value.getOrElse(interFeatures(j), 0)))
                  result.append(fMap.value.getOrElse(interFeatures(j), 0)).append(",")
              }
            }
            if (result.isEmpty) result.append("-1") else result.deleteCharAt(result.length - 1)
          } else {
            if (!features(i).toString.contains("-1") && fMap.value.get(features(i).toString) != None)
              //特征选择
              if (featureSelector.value.contains(fMap.value.getOrElse(features(i).toString, 0)))
                result.append(fMap.value.getOrElse(features(i).toString, 0))
            if (result.isEmpty) result.append("-1")
          }
          row += result.toString()
        }
        Row.fromSeq(row)
      }
    }
    resource.sqlContext.createDataFrame(rows, resource.schema)
  }

  /** json编码
    *
    * @param encodeDF
    * @return
    */
  def encodeToJson(encodeDF: DataFrame) = {
    var mtype = "u2f"
    var x_id = "uid"
    if (encodeDF.columns(0) == "cid") {
      mtype = "c2f"
      x_id = "cid"
    }
    encodeDF.map {
      case Row(benchmark: String, features@_*) => {
        val mtype_ = "\"" + "mtype" + "\"" + ":" + "\"" + mtype + "\""
        val id_ = "\"" + x_id + "\"" + ":" + "\"" + benchmark + "\""

        val features_ = new StringBuilder().append("\"" + "features" + "\"" + ":[")
        for (f <- features) {
          if (f.toString.contains(",")) {
            val interFeatures = f.toString.split(",")
            for (j <- 0 until interFeatures.length) {
              features_.append(interFeatures(j) + ",")
            }
          }
          if (!f.toString.contains(",") && !f.toString.contains("-1")) {
            features_.append(f.toString + ",")
          }
        }
        features_.deleteCharAt(features_.length - 1).append("]")

        val result = "{" + mtype_ + "," + id_ + "," + features_.toString() + "}"
        result
      }
    }
  }

  /** 过滤无效的记录（即全是无效特征的行）
    *
    * @param encodeDF
    * @return
    */
  def filter(encodeDF: DataFrame) = {
    val conditionExpr = new StringBuilder()
    for (col <- 1 until encodeDF.columns.length) {
      conditionExpr.append(encodeDF.columns(col) + " != '-1' or ")
    }
    conditionExpr.delete(conditionExpr.length - 4, conditionExpr.length)
    encodeDF.filter(conditionExpr.toString())
  }


  /**
    * 程序主入口
    */
  def main(args: Array[String]) {
    //hive sql
    val hql = args(0)
    //2017-05-026
    val day = args(1)
    //tuijina.tableName
    val saveTable = args(2)

    //卡方校验的特征选择结果
    val void_features = args(4)

    val sparkConf = new SparkConf().setAppName("EncodeSpark_fs")
    val sc = new SparkContext(sparkConf)

    //读取数据源
    val resource = getResource(sc, hql)

    var bucketedDF = new SQLContext(sc).emptyDataFrame
    //如果：style_clicks:0 30,60,90 | style_atimes:0 000,6000,9000
    if(args(3).contains("|")){
      val col_splits = args(3).split(" \\| ")
      val buckets = LinkedHashMap[(String, Int), Array[Double]]()
      for (s <- col_splits) {
        val col = s.split(" ")(0).split(":")(0)
        val identify = s.split(" ")(0).split(":")(1).toInt
        val splits = ArrayBuffer[Double]()
        splits += Double.NegativeInfinity
        for (split <- s.split(" ")(1).split(",")) {
          splits += split.toDouble
        }
        splits += Double.PositiveInfinity
        buckets += ((col, identify) -> splits.toArray)
      }
      bucketedDF = discretize1(resource, buckets)

    }else{
      //如果：style_clicks:0:5,style_atimes:0:5
      val col_numBuckets = args(3).split(",")
      val buckets = LinkedHashMap[(String, Int), Int]()
      for (s <- col_numBuckets) {
        val col = s.split(":")(0)
        val identify = s.split(":")(1).toInt
        val numBuckets = s.split(":")(2).toInt
        buckets += ((col, identify) -> numBuckets)
      }
      bucketedDF = discretize2(resource, buckets)
    }

    //读取有效特征文件
    val featureSelector = HashSet[Long]()
    for (line <- Source.fromFile(void_features).getLines()) {
      for(feature <- line.split(","))
      featureSelector +=(feature.toLong)
    }

    println("有效特征个数："+ featureSelector.size)


    //广播特征选择结果
    val features = sc.broadcast(featureSelector)

    //广播特征映射表
    val featureDF = getFeatureMap(sc, "tuijian.wireless_card_ctr_feature_code", day)
    val featureMap = featureDF.map(row => (row.getString(0), row.getLong(1))).collectAsMap()
    val fMap = sc.broadcast(featureMap)

    //one-hot编码
    val encodeDF = filter(oneHotEncode(bucketedDF, fMap, features))
    encodeDF.cache()

//    //1、发送mq一份
//    val json = encodeToJson(encodeDF)
//    json.foreach(MQSender.sendToMQ(_))
//    MQSender.close()

    //2、保存本地仓库一份
    encodeDF.registerTempTable("bucketed_table")
    encodeDF.sqlContext.sql("insert overwrite table " + saveTable + " partition(dt='" + day + "') " +
      "select * from bucketed_table")

    sc.stop()

  }
}

