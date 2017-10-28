package com.lakala.datacenter.faund

import java.util
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/7/28 0028.
  */
final object ScalaRandomForest {
  def main(args: Array[String]): Unit = {
    // spark context
    val sparkConf = new SparkConf().setAppName("JavaRandomForestExample")
    SparkConfUtil.setConf(sparkConf)
    val sc = new SparkContext(sparkConf)


    // Load and parse the data file.
    val datapath = "apply/data/svm"
    val data = MLUtils.loadLibSVMFile(sc, datapath)

    // Split the data into training and test sets (30% held out for testing)
    val splits = data.randomSplit(Array[Double](0.7, 0.3))
    val trainingData = splits(0)
    val testData = splits(1)

    // classification using RandomForest
    System.out.println("\nRunning example of classification using RandomForest\n")
    testClassification(trainingData, testData)

    // regression using RandomForest
    System.out.println("\nRunning example of regression using RandomForest\n")
    testRegression(trainingData, testData)

    sc.stop()
  }

  /**
    * Note: This example illustrates binary classification.
    * For information on multiclass classification, please refer to the JavaDecisionTree.java
    * example.
    *
    * @param trainingData
    * @param testData
    */
  def testClassification(trainingData: RDD[LabeledPoint], testData: RDD[LabeledPoint]): Unit = {


    // Train a RandomForest model.
    //  Empty categoricalFeaturesInfo indicates all features are continuous.
    val numClasses: Integer = 2

    // storing arity of categorical features. E.g., an entry (n -> k) indicates that feature n is categorical with k categories indexed from 0: {0, 1, ..., k-1}
    val categoricalFeaturesInfo: util.HashMap[Integer, Integer] = new util.HashMap[Integer, Integer]

    val numTrees: Integer = 3
    // Use more in practice.
    val featureSubsetStrategy: String = "auto"
    // Let the algorithm choose.
    val impurity: String = "gini"
    val maxDepth: Integer = 4
    val maxBins: Integer = 32
    val seed: Integer = 12345

    val model: RandomForestModel = RandomForest.trainClassifier(trainingData,
      numClasses, categoricalFeaturesInfo, numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins, seed)

    val predictionAndLabel: RDD[(Double, Double)] = testData.map(p => (model.predict(p.features), p.label))

    // compute test error
    val testErr: Double = 1.0 * predictionAndLabel.filter(pl => !pl._1.equals(pl._2)).count() / testData.count()

    System.out.println("Test Error: " + testErr)
    System.out.println("Learned classification forest model:\n" + model.toDebugString)
  }

  def testRegression(trainingData: RDD[LabeledPoint], testData: RDD[LabeledPoint]): Unit = {
    // Train a RandomForest model.
    //  Empty categoricalFeaturesInfo indicates all features are continuous.
    val categoricalFeaturesInfo = new util.HashMap[Integer, Integer]
    val numTrees = 3
    // Use more in practice.
    val featureSubsetStrategy = "auto"
    // Let the algorithm choose.
    val impurity = "variance"
    val maxDepth = 4
    val maxBins = 32
    val seed = 12345

    val model = RandomForest.trainRegressor(trainingData,
      categoricalFeaturesInfo, numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins, seed)

    // Evaluate model on test instances and compute test error
    val predictionAndLabel: RDD[(Double, Double)] = testData.map(p => (model.predict(p.features), p.label))

    val testMSE: Double = predictionAndLabel.map{pl =>
      val  diff = pl._1 - pl._2
      diff * diff}.reduce(_ + _) / testData.count()

    System.out.println("Test Mean Squared Error: " + testMSE)
    System.out.println("Learned regression forest model:\n" + model.toDebugString)
  }
}
