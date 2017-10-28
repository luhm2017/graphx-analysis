package com.lakala.datacenter.main

/**
  * Created by linyanshi on 2017/9/25 0025.
  */
object LiveCommunityDetection {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println(
        "Usage: LiveCommunityDetection <edge_list_file>\n" +
          "    --numEPart=<num_edge_partitions>\n" +
          "        The number of partitions for the graph's edge RDD.\n" +
          "    [--tol=<tolerance>]\n" +
          "        The tolerance allowed at convergence (smaller => more accurate). Default is " +
          "0.001.\n" +
          "    [--output=<output_file>]\n" +
          "        If specified, the file to write the ranks to.\n" +
          "    [--partStrategy=RandomVertexCut | EdgePartition1D | EdgePartition2D | " +
          "CanonicalRandomVertexCut]\n" +
          "        The way edges are assigned to edge partitions. Default is RandomVertexCut.")
      System.exit(-1)
    }
    //file/data/graphx/input/followers.txt -numEPart=100 -tol=0.001 -output=F:\idea_workspace\SparkLearning\outfile -partStrategy=RandomVertexCut
    Analytics.main(args.patch(0, List("pagerank"), 0))
  }
}
