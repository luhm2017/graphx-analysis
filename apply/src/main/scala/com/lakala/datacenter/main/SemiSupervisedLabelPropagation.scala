package com.lakala.datacenter.main

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.HashMap

/**
  * Created by linyanshi on 2017/9/19 0019.
  */
object SemiSupervisedLabelPropagation {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("SemiSupervisedLabelPropagation")
    val sc = new SparkContext(conf)
    val g = knnGraphApprox(getVertexKnnVertex, 4, sc)
    val gs = semiSupervisedLabelPropagation(g)
    println(knnPredict(gs, Array(30.0, 30.0)))
  }

  case class KnnVertex(classNum: Option[Int], pos: Array[Double]) extends Serializable {
    def dist(that: KnnVertex) =
      math.sqrt(pos.zip(that.pos).map(x => (x._1 - x._2) * (x._1 - x._2)).reduce(_ + _))
  }

  def getVertexKnnVertex(): Seq[KnnVertex] = {
    import scala.util.Random
    Random.setSeed(17L)
    val n = 10
    val a = (1 to n * 2).map(i => {
      val x = Random.nextDouble;
      if (i <= n)
        KnnVertex(if (i % n == 0) Some(0) else None, Array(x * 50,
          20 + (math.sin(x * math.Pi) + Random.nextDouble / 2) * 25))
      else
        KnnVertex(if (i % n == 0) Some(1) else None, Array(x * 50 + 25,
          30 - (math.sin(x * math.Pi) + Random.nextDouble / 2) * 25))
    })
    a
  }

  def knnGraphApprox(a: Seq[KnnVertex], k: Int, sc: SparkContext) = {
    val a2 = a.zipWithIndex.map(x => (x._2.toLong, x._1)).toArray
    val v = sc.makeRDD(a2)
    val n = 3
    val minMax =
      v.map(x => (x._2.pos(0), x._2.pos(0), x._2.pos(1), x._2.pos(1)))
        .reduce((a, b) => (math.min(a._1, b._1), math.max(a._2, b._2),
          math.min(a._3, b._3), math.max(a._4, b._4)))
    val xRange = minMax._2 - minMax._1
    val yRange = minMax._4 - minMax._3

    def calcEdges(offset: Double) =
      v.map(x => (math.floor((x._2.pos(0) - minMax._1)
        / xRange * (n - 1) + offset) * n
        + math.floor((x._2.pos(1) - minMax._3)
        / yRange * (n - 1) + offset),
        x))
        .groupByKey(n * n)
        .mapPartitions(ap => {
          val af = ap.flatMap(_._2).toList
          af.map(v1 => (v1._1, af.map(v2 => (v2._1, v1._2.dist(v2._2)))
            .toArray
            .sortWith((e, f) => e._2 < f._2)
            .slice(1, k + 1)
            .map(_._1)))
            .flatMap(x => x._2.map(vid2 => Edge(x._1, vid2,
              1 / (1 + a2(vid2.toInt)._2.dist(a2(x._1.toInt)._2)))))
            .iterator
        })

    val e = calcEdges(0.0).union(calcEdges(0.5))
      .distinct
      .map(x => (x.srcId, x))
      .groupByKey
      .map(x => x._2.toArray
        .sortWith((e, f) => e.attr > f.attr)
        .take(k))
      .flatMap(x => x)
    Graph(v, e)
  }


  def semiSupervisedLabelPropagation(g: Graph[KnnVertex, Double], maxIterations: Int = 0) = {

    val maxIter = if (maxIterations == 0) g.vertices.count / 2
    else maxIterations

    var g2 = g.mapVertices((vid, vd) => (vd.classNum.isDefined, vd))
    var isChanged = true
    var i = 0
    do {
      val newV =
        g2.aggregateMessages[Tuple2[Option[Int], HashMap[Int, Double]]](
          ctx => {
            ctx.sendToSrc((ctx.srcAttr._2.classNum,
              if (ctx.dstAttr._2.classNum.isDefined)
                HashMap(ctx.dstAttr._2.classNum.get -> ctx.attr)
              else
                HashMap[Int, Double]()))
            if (ctx.srcAttr._2.classNum.isDefined)
              ctx.sendToDst((None,
                HashMap(ctx.srcAttr._2.classNum.get -> ctx.attr)))
          },
          (a1, a2) => {
            if (a1._1.isDefined)
              (a1._1, HashMap[Int, Double]())
            else if (a2._1.isDefined)
              (a2._1, HashMap[Int, Double]())
            else
              (None, (a1._2 ++ a2._2.map {
                case (k, v) => k -> (v + a1._2.getOrElse(k, 0.0))
              }).asInstanceOf[HashMap[Int, Double]])
          }
        )

      val newVClassVoted = newV.map(x => (x._1,
        if (x._2._1.isDefined)
          x._2._1
        else if (x._2._2.size > 0)
          Some(x._2._2.toArray.sortWith((a, b) => a._2 > b._2)(0)._1)
        else None
      ))

      isChanged = g2.vertices.join(newVClassVoted)
        .map(x => x._2._1._2.classNum != x._2._2)
        .reduce(_ || _)

      g2 = g2.joinVertices(newVClassVoted)((vid, vd1, u) =>
        (vd1._1, KnnVertex(u, vd1._2.pos)))
      i += 1
    } while (i < maxIter && isChanged)

    g2.mapVertices((vid, vd) => vd._2)
  }

  def knnPredict[E](g: Graph[KnnVertex, E], pos: Array[Double]) =
    g.vertices
      .filter(_._2.classNum.isDefined)
      .map(x => (x._2.classNum.get, x._2.dist(KnnVertex(None, pos))))
      .min()(new Ordering[Tuple2[Int, Double]] {
        override def compare(a: Tuple2[Int, Double], b: Tuple2[Int, Double]): Int =
          a._2.compare(b._2)
      })._1
}
