package com.lakala.datacenter.faund

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by Administrator on 2017/7/28 0028.
  */
object Titanic {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: Titanic <input_file>")
      System.exit(1)
    }

    val inputFile: String = args(0)
    val sparkConf: SparkConf = new SparkConf().setAppName("Titanic")
    SparkConfUtil.setConf(sparkConf)

    val sc: SparkContext = new SparkContext(sparkConf)
    val sqlContext: SQLContext = new SQLContext(sc)
    val results: DataFrame = DatasetTitanic.createDF(sqlContext, inputFile)

    results.printSchema

    val data: RDD[LabeledPoint] = DatasetTitanic.createLabeledPointsRDD(sc, sqlContext, inputFile)
    val splits: Array[RDD[LabeledPoint]] = data.randomSplit(Array[Double](0.7, 0.3))
    val trainingData: RDD[LabeledPoint] = splits(0)
    val testData: RDD[LabeledPoint] = splits(1)

    System.out.println("\nRunning example of classification using RandomForest\n")
    ScalaRandomForest.testClassification(trainingData, testData)

    System.out.println("\nRunning example of regression using RandomForest\n")
    ScalaRandomForest.testRegression(trainingData, testData)

    sc.stop
  }
}
