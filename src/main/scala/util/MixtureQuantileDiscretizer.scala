package util

import org.apache.spark.ml.feature.{QuantileDiscretizer}
import org.apache.spark.sql.DataFrame

/**
  * Created by dengxing on 2017/6/16.
  */
class MixtureQuantileDiscretizer {

  private[this] var benchmark: String = _
  private[this] var identify: Int = 0
  private[this] var inputCol: String = _
  private[this] var outputCol: String = _
  private[this] var numBuckets: Int = 5

  def setSplits(value: Int): this.type = {
    this.numBuckets = value
    this
  }

  def setInputCol(value: String): this.type = {
    this.inputCol = value
    this
  }

  def setOutputCol(value: String): this.type = {
    this.outputCol = value
    this
  }

  def setIdentify(value: Int): this.type = {
    this.identify = value
    this
  }

  def setBenchmark(value: String): this.type = {
    this.benchmark = value
    this
  }

  def transform(dataset: DataFrame): DataFrame = {

    if (identify == 0) {
      dataset.registerTempTable("tmpTable1")
      val hql = "select " + benchmark + ",split(a_b,\":\")[0] as a,cast(split(a_b,\":\")[1] as double) as b from (select " + benchmark + "," + inputCol + " from tmpTable1)t lateral view explode(split(" + inputCol + ",',')) tmp as a_b"
      val table1 = dataset.sqlContext.sql(hql)

      val discretizer = new QuantileDiscretizer()
        .setInputCol("b")
        .setOutputCol("bucketed_b")
        .setNumBuckets(numBuckets)

      val discretizedDF = discretizer.fit(table1).transform(table1)

      discretizedDF.select(discretizedDF.col(benchmark), discretizedDF.col("a"), discretizedDF.col("bucketed_b").cast("int")).registerTempTable("tmpTable2")
      discretizedDF.sqlContext.sql("select " + benchmark + ",concat(a,'_',bucketed_b) as bucketed_c from tmpTable2").registerTempTable("tmpTable3")
      val tmpDF = discretizedDF.sqlContext.sql("select " + benchmark + ",concat_ws(',',collect_set(bucketed_c)) as " + outputCol + " from tmpTable3  group by " + benchmark)
      tmpDF

    } else {
      dataset.registerTempTable("tmpTable")
      val hql = "select " + benchmark + ",cast(" + inputCol + " as double) as " + inputCol + " from tmpTable"
      val table = dataset.sqlContext.sql(hql)

      val discretizer = new QuantileDiscretizer()
        .setInputCol(inputCol)
        .setOutputCol("bucketed")
        .setNumBuckets(numBuckets)

      val discretizedDF = discretizer.fit(table).transform(table)

      discretizedDF.select(discretizedDF.col(benchmark), discretizedDF.col("bucketed").cast("int")).registerTempTable("tmpTable_")
      val tmpDF = discretizedDF.sqlContext.sql("select " + benchmark + ",concat('" + inputCol + "','_'," + "bucketed) as " + outputCol + " from tmpTable_")
      tmpDF
    }
  }

}
