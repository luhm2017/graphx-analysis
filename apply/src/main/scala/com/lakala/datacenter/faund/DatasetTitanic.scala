package com.lakala.datacenter.faund

import java.util

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by Administrator on 2017/7/28 0028.
  */
object DatasetTitanic {
  def createDF(sqlContext: SQLContext, inputFile: String): DataFrame = { // options
    val options = new util.HashMap[String, String]
    options.put("header", "true")
    options.put("path", inputFile)
    options.put("delimiter", ",")
    // create dataframe from input file
    val df = sqlContext.load("com.databricks.spark.csv", options)
    df.printSchema()
    df
  }

  // create an RDD of Vectors from a DataFrame
  def createLabeledPointsRDD(ctx: SparkContext, sqlContext: SQLContext, inputFile: String): RDD[LabeledPoint] = {
    val df = createDF(sqlContext, inputFile)
    // convert dataframe to an RDD of Vectors
    df.map { row =>
      val survived = row.getString(1).toInt
      val arr = new Array[Double](2)
      arr(0) = toDouble(row.getString(5))
      arr(1) = toDouble(row.getString(6))
      new LabeledPoint(survived, Vectors.dense(arr))
    }
  }

  def toDouble = (value: String) => {
    if (value.length == 0) 0.0 else value.toDouble
  }
}
