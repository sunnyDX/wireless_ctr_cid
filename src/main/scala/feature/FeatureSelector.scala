package feature

/**
  * Created by dengxing on 2017/6/13.
  */

import java.io.{File, PrintWriter}

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.ChiSqSelector
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object FeatureSelector {

  def main(args: Array[String]): Unit = {
    val input = args(0)
    val output = args(1)

    val conf = new SparkConf().setAppName("features_select").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val allData = sqlContext.read.format("libsvm").load(input)

    val data1 = allData.where("label=1").limit(100000)
    val data2 = allData.where("label=0").limit(100000)

    val data = data1.unionAll(data2)

    val selector = new ChiSqSelector()
      .setNumTopFeatures(10)
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setOutputCol("selectedFeatures")

    val selectorModel = selector.fit(data)

    //保存到本地文件
    val writer = new PrintWriter(new File(output))
    println("选择的特征：" + selectorModel.selectedFeatures.mkString(","))
    writer.write(selectorModel.selectedFeatures.mkString(","))
    writer.close()

    sc.stop()
  }

}
