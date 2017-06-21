package tag

/**
  * Created by dengxing on 2017/4/13.
  */

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.{SparkConf, SparkContext}
import util.MQSender

object GenerateUserTag {

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
      "uid," +
      "style_clicks," +
      "style_atimes" +
      " from " + table + " where dt ='" + day + "'")
    resource
  }

  /** 基于原始数据对每个uid召回N个Tag
    *
    * @param clicks
    * @param atimes
    * @param N
    * @return
    */
  def getUidTag(clicks: String, atimes: String, tagPrefix: String, N: Int = 1): List[(String, String)] = {

    val tag_clicks = clicks.split(",").filter(!_.contains("-1"))
    val tag_atimes = atimes.split(",").filter(!_.contains("-1")).filter(!_.split(":")(1).equals("0"))

    if (tag_clicks.length != 0 && tag_atimes.length != 0) {
      val max_clicks = tag_clicks.map(_.split(":")(1).toDouble).max
      val max_atimes = tag_atimes.map(_.split(":")(1).toDouble).max

      val tag_clicks_map = tag_clicks.map(_.split("_")(1))
        .map(v => (v.split(":")(0), v.split(":")(1).toDouble / max_clicks))
        .toMap

      val tag_atimes_map = tag_atimes.map(_.split("_")(1))
        .map(v => (v.split(":")(0), v.split(":")(1).toDouble / max_atimes))
        .toMap

      val intersect_key = tag_clicks_map.keySet.intersect(tag_atimes_map.keySet)

      if (intersect_key.size != 0) {
        val tag_score = intersect_key.map(k => (k, tag_clicks_map.getOrElse(k, 0.0) + tag_atimes_map.getOrElse(k, 0.0))).toList
        val score_max = tag_score.map(_._2).max
        tag_score.sortWith((a, b) => a._2 > b._2).map(v => (tagPrefix + "_" + v._1, "%1.4f".format(v._2 / score_max))).take(N)
      } else {
        List.empty
      }
    } else {
      List.empty
    }
  }

  /** json 编码
    *
    * @param uid_tag_score
    */
  def encodeToJson(uid_tag_score: (String, List[(String, String)])) = {
    val mtype = "u2p"
    val mtype_ = "\"" + "mtype" + "\"" + ":" + "\"" + mtype + "\""

    val uid = uid_tag_score._1
    val uid_ = "\"" + "uid" + "\"" + ":" + "\"" + uid + "\""

    val tag_score = uid_tag_score._2
    val profile_ = new StringBuilder().append("\"" + "profile" + "\"" + ":[")
    for (v <- tag_score) {
      val tag = v._1
      val score = v._2
      val tag_score = "[" + "\"" + tag + "\"" + "," + score + "]"
      profile_.append(tag_score + ",")
    }
    profile_.deleteCharAt(profile_.length - 1).append("]")
    val result = "{" + mtype_ + "," + uid_ + "," + profile_.toString() + "}"
    result
  }

  /** 对数据源处理
    *
    * @param resource
    * @return
    */

  def dealResource(resource: DataFrame) = {
    resource.map(row => {
      val uid = row.getString(0)
      //---------------------------------------
      val style_clicks = row.getString(1)
      val style_atimes = row.getString(2)
      val style_score = getUidTag(style_clicks, style_atimes, "style", 3)
      //---------------------------------------

      (uid, style_score)

    }).filter(!_._2.isEmpty).map(encodeToJson(_))
  }


  /**
    * 主程序
    *
    */

  def main(args: Array[String]): Unit = {
    val table = args(0) //要处理的表名
    val day = args(1) //当前的日期
    val conf = new SparkConf().setAppName("GenerateUserTag")
    val sc = new SparkContext(conf)
    val resource = getResource(sc, table, day)
    val json = dealResource(resource)
    json.foreach(MQSender.sendToMQ(_))
    MQSender.close()
    sc.stop()
  }
}

