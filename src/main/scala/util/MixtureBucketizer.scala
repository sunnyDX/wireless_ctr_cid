package util

import org.apache.spark.ml.feature.Bucketizer
import org.apache.spark.sql.DataFrame


/**
  * Created by dengxing on 2017/5/26.
  */
class MixtureBucketizer() {

  private[this] var benchmark: String = _
  private[this] var identify: Int = 0
  private[this] var inputCol: String = _
  private[this] var outputCol: String = _
  private[this] var splits: Array[Double] = Array.emptyDoubleArray

  def setSplits(value: Array[Double]): this.type = {
    this.splits = value.clone()
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
      val bucketizer = new Bucketizer()
        .setInputCol("b")
        .setOutputCol("bucketed_b")
        .setSplits(splits)
      val bucketedDF = bucketizer.transform(table1)
      bucketedDF.select(bucketedDF.col(benchmark), bucketedDF.col("a"), bucketedDF.col("bucketed_b").cast("int")).registerTempTable("tmpTable2")
      bucketedDF.sqlContext.sql("select " + benchmark + ",concat(a,'_',bucketed_b) as bucketed_c from tmpTable2").registerTempTable("tmpTable3")
      val tmpDF = bucketedDF.sqlContext.sql("select " + benchmark + ",concat_ws(',',collect_set(bucketed_c)) as " + outputCol + " from tmpTable3  group by " + benchmark)
      tmpDF
    } else {
      dataset.registerTempTable("tmpTable")
      val hql = "select " + benchmark + ",cast(" + inputCol + " as double) as " + inputCol + " from tmpTable"
      val table = dataset.sqlContext.sql(hql)
      val bucketizer = new Bucketizer()
        .setInputCol(inputCol)
        .setOutputCol("bucketed")
        .setSplits(splits)
      val bucketedDF = bucketizer.transform(table)
      bucketedDF.select(bucketedDF.col(benchmark), bucketedDF.col("bucketed").cast("int")).registerTempTable("tmpTable_")
      val tmpDF = bucketedDF.sqlContext.sql("select " + benchmark + ",concat('" + inputCol + "','_'," + "bucketed) as " + outputCol + " from tmpTable_")
      tmpDF
    }
  }
}
