package model

/**
  * Created by dengxing on 2017/4/13.
  */

import java.io
import java.io.{File, PrintWriter}

import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object LR_Train {

  def main(args: Array[String]): Unit = {
    val input = args(0)
    val output = args(1)
    val conf = new SparkConf().setAppName("LogisticRegression_cid_train")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val data = sqlContext.read.format("libsvm").load(input)


    //训练样本、测试样本划分
    val Array(trainingData, testData) = data.randomSplit(Array(0.8, 0.2))


    //模型定义
    val lr = new LogisticRegression()
      .setMaxIter(200)
      .setElasticNetParam(1.0)

    //训练
    val lorModel = lr.fit(trainingData)


    //预测
    val predictions = lorModel.transform(testData)
    val trainingSummary = lorModel.summary


    //输出损失率
    val objectiveHistory = trainingSummary.objectiveHistory
    objectiveHistory.foreach(loss => println("loss : " + loss))

    predictions.cache()

    //模型评估
    val evaluator = new BinaryClassificationEvaluator()
      .setLabelCol("label")
      .setMetricName("areaUnderROC")

    val areaUnderROC = evaluator.evaluate(predictions)
    println("Test areaUnderROC = " + areaUnderROC)

    val evaluator1 = new BinaryClassificationEvaluator()
      .setLabelCol("label")
      .setMetricName("areaUnderPR")

    val areaUnderPR = evaluator1.evaluate(predictions)
    println("Test areaUnderPR = " + areaUnderPR)

    /* 基于业务，对模型输出进行json编码
       包含字段及说明：
       1> type: 数据类型（0代表稀疏编码，1代表稠密编码）
       2> size: 模型特征维度
       3> indices：模型特征索引，从0开始
       4> values：模型特征的权重值
       5> intercept：模型截距
      */
    val save_model = lorModel.coefficients.toJson
      .dropRight(1)
      .concat(",")
      .concat("\"intercept\":")
      .concat(lorModel.intercept.toString)
      .concat("}")

    val file = new File(output)

    //保存模型到本地文件，方便服务端读取
    var writer = new PrintWriter(file)
    writer.write(save_model)
    writer.close()

    //记录最新最新模型文件名称到lastestModel.txt
    val parentFile = file.getAbsoluteFile.getParentFile.getParentFile
    writer = new PrintWriter(new File(parentFile.getAbsolutePath+"/lastestModel.txt"))
    writer.write(output.substring(output.lastIndexOf("/")+1))
    writer.close()

    sc.stop()
  }
}

