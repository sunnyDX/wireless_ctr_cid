package model

/**
  * Created by dengxing on 2017/4/13.
  */

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.{SparkConf, SparkContext}

object Hive2LibsvmSpark {


  /** 基于dt时间获取原始数据源
    *
    * @param sc        SparkContext
    * @param table     转换的hive表
    * @param day 获取当前日期的数据
    * @return 原始数据的dataFrame
    */
  def getResource(sc: SparkContext, table: String, day: String) = {
    val hiveContext = new HiveContext(sc)
    import hiveContext.sql
    val resource = sql("select " +
      "label," +
      "style_clicks," +
      "style_atimes," +
      "search_style_atimes," +
      "type_clicks," +
      "type_atimes," +
      "search_type_atimes," +
      "area_clicks," +
      "area_atimes," +
      "search_area_atimes," +
      "sectionid_clicks," +
      "fcid," +
      "areaid," +
      "styleid," +
      "typeid," +
      "displaycnt," +
      "vvcnt," +
      "vvsec," +
      "funclickcnt," +
      "conclickcnt" +
      " from " + table + " where dt ='" + day + "'")
    resource
  }

  /** 给予特征索引对libsvm进行排序
    *
    * @param str 待排序的字符串
    * @return libsvm排序后的字符串
    */
  def sortByIndex(str: String): String = {
    val label = str.split(" ").head
    val feature = str.split(" ").tail
    val sortedFeature = feature.sortWith((a, b) => a.split(":")(0).toInt < b.split(":")(0).toInt)
    label + " " + sortedFeature.mkString(" ")
  }


  /** 处理数据源为LibSVM格式
    *
    * @param resource
    * @return LibSVM格式的RDD[String]
    */
  def transform(resource: DataFrame) = {

    /** 转化数据为LibSVM格式
      *
      * @param result
      * @param features
      * @return libSVM格式的字符串
      */
    def dealFeatures(result: StringBuilder, features: Seq[Any]) = {

      for (i <- 0 until features.length) {
        if (features(i).toString.contains(",")) {
          val interFeatures = features(i).toString.split(",")
          for (j <- 0 until interFeatures.length) {
            result.append(" ").append((interFeatures(j).toInt + 1) + ":1")
          }
        } else {
          if (!features(i).toString.contains("-1")) {
            result.append(" ").append((features(i).toString.toInt + 1) + ":1")
          }
        }
      }

      sortByIndex(result.toString())
    }

    resource.map {
      //模式匹配--训练数据（label:Int,features ....）
      case Row(label: Int, features@_*) => {
        val result = new StringBuilder(label.toString)
        dealFeatures(result, features)
      }
    }
  }


  /**
    * 主程序
    *
    */

  def main(args: Array[String]): Unit = {
    val output = args(0) //样本输出路径
    val table = args(1) //要处理的表名
    val day = args(2) //前N天的日期
    val sparkConf = new SparkConf().setAppName("Hive2LibsvmSpark")
    val sc = new SparkContext(sparkConf)
    val resource = getResource(sc, table, day)
    val libSVM = transform(resource)
    libSVM.saveAsTextFile(output)
    sc.stop()
  }
}

