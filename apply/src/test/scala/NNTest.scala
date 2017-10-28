/*
///**
//  * Created by Administrator on 2017/8/4 0004.
//  */
//import breeze.linalg.{DenseMatrix => BDM, max => Bmax, min => Bmin}
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.{SparkConf, SparkContext}
//
//import scala.collection.mutable.ArrayBuffer
//
///**
//  * Created by Administrator on 2017/7/27.
//  */
//object NNTest {
//
//  def main(args: Array[String]) = {
//    // 设置运行环境
//    val conf = new SparkConf().setAppName("Neural Net")
//      .setMaster("spark://master:7077").setJars(Seq("E:\\Intellij\\Projects\\MachineLearning\\MachineLearning.jar"))
//    val sc = new SparkContext(conf)
//    Logger.getRootLogger.setLevel(Level.WARN)
//
//    // 随机生成样本数据
//    Logger.getRootLogger.setLevel(Level.WARN)
//    val sampleRow = 1000
//    val sampleColumn = 5
//    val randSamp_01 = RandSampleData.RandM(sampleRow, sampleColumn, -10, 10, "sphere")
//    // 归一化
//    val norMax = Bmax(randSamp_01(::, breeze.linalg.*))
//    val norMin = Bmin(randSamp_01(::, breeze.linalg.*))
//    val nor1 = randSamp_01 - (BDM.ones[Double](randSamp_01.rows, 1)) * norMin
//    val nor2 = nor1 :/ ((BDM.ones[Double](nor1.rows, 1)) * (norMax - norMin))
//    // 转换样本
//    val randSamp_02 = ArrayBuffer[BDM[Double]]()
//    for (i <- 0 to sampleRow - 1) {
//      val mi = nor2(i, ::)
//      val mi1 = mi.inner
//      val mi2 = mi1.toArray
//      val mi3 = new BDM(1, mi2.length, mi2)
//      randSamp_02 += mi3
//    }
//    val randSamp_03 = sc.parallelize(randSamp_02, 10)
//    sc.setCheckpointDir("hdfs://master:9000/ml/data/checkpoint")
//    randSamp_03.checkpoint()
//    val trainRDD = randSamp_03.map(f => (new BDM(1, 1, f(::, 0).data), f(::, 1 to -1)))
//    // 训练，建立模型
//    val opts = Array(100.0, 50.0, 0.0)
//    trainRDD.cache
//    val numExamples = trainRDD.count()
//    println(s"Number of Examples: $numExamples")
//    val NNModel = new NeuralNet().
//      setSize(Array(5, 10, 10, 10, 10, 10, 1)).
//      setLayer(7).
//      setActivation_function("tanh_opt").
//      setLearningRate(2.0).
//      setScaling_learningRate(1.0).
//      setWeightPenaltyL2(0.0).
//      setNonSparsityPenalty(0.0).
//      setSparsityTarget(0.05).
//      setInputZeroMaskedFraction(0.0).
//      setDropoutFraction(0.0).
//      setOutput_function("sigm").
//      NNtrain(trainRDD, opts)
//
//    // 测试模型
//    val NNPrediction = NNModel.predict(trainRDD)
//    val NNPredictionError = NNModel.Loss(NNPrediction)
//    println(s"NNerror = $NNPredictionError")
//    val showPrediction = NNPrediction.map(f => (f.label.data(0), f.predict_label.data(0))).take(100)
//    println("Prediction Result")
//    println("Value" + "\t" + "Prediction" + "\t" + "Error")
//    for (i <- 0 until showPrediction.length)
//      println(showPrediction(i)._1 + "\t" + showPrediction(i)._2 + "\t" + (showPrediction(i)._2 - showPrediction(i)._1))
//
//    var tmpWeight = NNModel.weights(0)
//    for (i <-0 to 5) {
//      tmpWeight = NNModel.weights(i)
//      println(s"Weight of Layer ${i+1}")
//      for (j <- 0 to tmpWeight.rows - 1) {
//        for (k <- 0 to tmpWeight.cols - 1) {
//          print(tmpWeight(j, k) + "\t")
//        }
//        println()
//      }
//    }
//
//  }
//
//}

dstream.foreachRDD(rdd => {
  //embedded function
  def func(records: Iterator[String]) {
    var conn: Connection = null
    var stmt: PreparedStatement = null
    try {
      val url = "jdbc:mysql://192.168.26.140:3306/person";
      val user = "root";
      val password = ""
      conn = DriverManager.getConnection(url, user, password)
      records.flatMap(_.split(" ")).foreach(word => {
        val sql = "insert into TBL_WORDS(word) values (?)";
        stmt = conn.prepareStatement(sql);
        stmt.setString(1, word)
        stmt.executeUpdate();
      })
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (stmt != null) {
        stmt.close()
      }
      if (conn != null) {
        conn.close()
      }
    }
  }

  val repartitionedRDD = rdd.repartition(3)
  repartitionedRDD.foreachPartition(func)
})
*/
