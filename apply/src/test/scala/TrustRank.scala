import org.apache.spark.Logging
import org.apache.spark.graphx._

import scala.reflect.ClassTag
import scala.util.Random

/**
  * Created by linyanshi on 2017/9/19 0019.
  */
object TrustRank extends Logging {

  /*
   * VD : (double, double) denotes rank and score
   * ED : double , not used
   */
  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], numIter: Int): Long = {
    val resetProb: Double = 0.15
    val resetRank: Double = 0.15

    def resetScore: Double = Random.nextDouble()


    var rankGraph: Graph[Double, Double] = graph
      .outerJoinVertices(graph.outDegrees) { (vid, vd, deg) => deg.getOrElse(0) }
      .mapTriplets(e => 1.0 / e.srcAttr, TripletFields.Src)
      .mapVertices((id, attr) => resetRank)

    val scoreGraph: Graph[Double, _] = graph.mapVertices((id, attr) => resetScore).cache()

    var iteration = 0

    val start_ms = System.currentTimeMillis()
    println("Start time : " + start_ms)

    while (iteration < numIter) {
      val rankUpdates = rankGraph.aggregateMessages[Double](
        ctx => ctx.sendToDst(ctx.srcAttr * ctx.attr),
        _ + _,
        TripletFields.Src
      )

      // update rank and apply
      rankGraph = rankGraph.joinVertices(rankUpdates) {
        (id, old_vd, msgSum) => (1.0 - resetProb) * msgSum
      }.joinVertices(scoreGraph.vertices) {
        (id, rank, score) => (rank + resetProb * score)
      }

      rankGraph.vertices.count() // materialize rank graph
      logInfo(s"TrustRank finished iteration $iteration.")

      iteration += 1

    }


    var end_ms = System.currentTimeMillis()
    println("End time : " + end_ms)

    println("Cost : " + (end_ms - start_ms))

    end_ms - start_ms
  }

}
