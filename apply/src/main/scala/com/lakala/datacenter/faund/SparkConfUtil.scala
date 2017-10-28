package com.lakala.datacenter.faund

import org.apache.spark.SparkConf

/**
  * Created by Administrator on 2017/7/28 0028.
  */
object SparkConfUtil {
  val isLocal = true;

  def setConf(conf: SparkConf): Unit = {

    if (isLocal) {
      conf.setMaster("local")
      conf.set("spark.broadcast.compress", "false")
      conf.set("spark.shuffle.compress", "false")
    }
  }
}
