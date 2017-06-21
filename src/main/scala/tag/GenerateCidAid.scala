package tag

/**
  * Created by dengxing on 2017/4/13.
  */

import java.io.{File, PrintWriter}

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import util.MQSender

object GenerateCidAid {

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
    val resource = sql(
      "select cid,concat_ws(',',collect_set(aid_sorting)) as aid_sorting" +
        " from" +
        " (select cid,concat(aid,':',sorting) as aid_sorting" +
        " from" +
        " (select  cid,cid_aid.aid as aid,sorting,row_number() over (partition by cid order by sorting) rank" +
        " from" +
        " (select cid,aid" +
        " from " + table +
        " where dt ='" + day + "'" +
        " )cid_aid" +
        " join" +
        " (select aid,cast(sorting as int) as sorting" +
        " from wireless.rank_working_ori" +
        " where dt ='" + day + "'" +
        " )aid_sorting" +
        " on cid_aid.aid = aid_sorting.aid" +
        " group by cid,cid_aid.aid,sorting" +
        " )t" +
        " where rank <= 50" +
        " group by cid,aid,sorting" +
        " )main" +
        " group by cid")
    resource
  }

  /**
    * 基于原始数据对每个Tag召回N个Cid
    *
    * @param aid_score_raw 属于数据列
    * @param N             召回个数
    * @return (cid,score)的列表
    */
  def getCidAid(aid_score_raw: String, N: Int = 10): List[(String, String)] = {
    val aid_score_list = aid_score_raw.split(",")

    val min_score = aid_score_list.map(_.split(":")(1).toDouble).min

    aid_score_list.sortWith((a, b) => a.split(":")(1).toDouble < b.split(":")(1).toDouble)
      .take(N)
      .map(l => (l.split(":")(0), "%1.4f".format(min_score / l.split(":")(1).toDouble))).toList
  }


  /** json 编码并发送 MQ
    *
    * @param cid_aid_score
    */
  def encodeToJson(cid_aid_score: (String, List[(String, String)])) = {
    val cid = cid_aid_score._1
    val cid_ = "\"" + "cid" + "\"" + ":" + "\"" + cid + "\""

    val aid_score = cid_aid_score._2
    val aids_ = new StringBuilder().append("\"" + "aids" + "\"" + ":[")
    for (v <- aid_score) {
      val cid = v._1
      val score = v._2
      val cid_score = "[" + "\"" + cid + "\"" + "," + score + "]"
      aids_.append(cid_score + ",")
    }
    aids_.deleteCharAt(aids_.length - 1).append("]")

    val result = "{"  + cid_ + "," + aids_.toString() + "}\n"
    result
  }

  /**
    * 对数据源处理
    *
    * @param resource
    * @return
    */
  def dealResource(resource: DataFrame) = {
    resource.map(row => {
      val cid = row.getString(0)
      val aid_score_raw = row.getString(1)
      val aid_score = getCidAid(aid_score_raw,50)
      (cid, aid_score)
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
    val output = args(2) //保存目录
    val conf = new SparkConf().setAppName("GenerateCidAid")
    val sc = new SparkContext(conf)
    val resource = getResource(sc, table, day)
    val json = dealResource(resource)


    //保存到本地文件，方便服务端读取
    val writer = new PrintWriter(new File(output))
    json.collect().foreach(writer.write(_))
    writer.close()
    sc.stop()
  }

}

