package tag

/**
  * Created by dengxing on 2017/4/13.
  */

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.{SparkConf, SparkContext}
import util.MQSender

object GenerateTagCid {

  /** 基于dt时间获取原始数据源
    *
    * @param sc    SparkContext
    * @param table 转换的hive表
    * @param day   获取分区为 day 的数据
    * @return 原始数据的dataFrame
    */
  def getResource(sc: SparkContext, table: String, day: String) = {
    val hiveContext = new HiveContext(sc)
    import hiveContext.sql
    val resource = sql("select " +
      "style," +
      "cid_score" +
      " from " + table + " where dt ='" + day + "'")
    resource
  }

  /**
    * 基于原始数据对每个Tag召回N个Cid
    *
    * @param cid_score_raw 属于数据列
    * @param N             召回个数
    * @return (cid,score)的列表
    */
  def getTagCid(cid_score_raw: String, N: Int = 10): List[(String, String)] = {
    val cid_score_list = cid_score_raw.split(",")

    val min_score = cid_score_list.map(_.split(":")(1).toDouble).min

    cid_score_list.sortWith((a, b) => a.split(":")(1).toDouble < b.split(":")(1).toDouble)
      .take(N)
      .map(l => (l.split(":")(0), "%1.4f".format(min_score / l.split(":")(1).toDouble))).toList
  }


  /** json 编码并发送 MQ
    *
    * @param tag_cid_score
    */
  def encodeToJson(tag_cid_score: (String, List[(String, String)])) = {
    val mtype = "p2c"
    val mtype_ = "\"" + "mtype" + "\"" + ":" + "\"" + mtype + "\""

    val tag = tag_cid_score._1
    val profile_ = "\"" + "profile" + "\"" + ":" + "\"" + tag + "\""

    val tag_score = tag_cid_score._2
    val cids_ = new StringBuilder().append("\"" + "cids" + "\"" + ":[")
    for(v <-tag_score){
      val cid = v._1
      val score = v._2
      val cid_score = "[" + "\"" + cid + "\"" + "," + score + "]"
      cids_.append(cid_score + ",")
    }
    cids_.deleteCharAt(cids_.length - 1).append("]")

    val result = "{" + mtype_ + "," + profile_ + "," + cids_.toString() + "}"
    result
  }

  /**
    * 对数据源处理
    *
    * @param resource
    * @return
    */
  def dealResource(resource: DataFrame, tagPrefix: String) = {
    resource.map(row => {
      val tag = row.getString(0)
      val cid_score_raw = row.getString(1)
      val cid_score = getTagCid(cid_score_raw)
      (tagPrefix + "_" + tag, cid_score)
    }
    ).map(encodeToJson(_))
  }

  /**
    * 主程序
    *
    */

  def main(args: Array[String]): Unit = {
    val table = args(0) //要处理的表名
    val day = args(1) //当前的日期
    val conf = new SparkConf().setAppName("GenerateTagCid")
    val sc = new SparkContext(conf)
    val resource = getResource(sc, table, day)
    val json = dealResource(resource,"style")
    json.foreach(MQSender.sendToMQ(_))
    MQSender.close()
    sc.stop()
  }

}

