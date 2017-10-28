package com.lakala.datacenter.core.processor;

import com.lakala.datacenter.core.algorithms.Algorithms;
import com.lakala.datacenter.core.config.ConfigurationLoader;
import com.lakala.datacenter.core.models.ProcessorMessage;
import com.lakala.datacenter.core.models.ProcessorMode;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.concurrent.TimeoutException;

public class GraphProcessor {

    public static final String PROPERTY_GRAPH_UPDATE_PATH = "/neo4j/mazerunner/propertyUpdateList.txt";
    public static final String PARTITIONED_PROPERTY_GRAPH_UPDATE_PATH = "/neo4j/mazerunner/update/jobs/{job_name}/propertyUpdateList.txt";

    public static final String TRIANGLE_COUNT = "triangle_count";
    public static final String CONNECTED_COMPONENTS = "connected_components";
    public static final String PAGERANK = "pagerank";
    public static final String STRONGLY_CONNECTED_COMPONENTS = "strongly_connected_components";
    public static final String CLOSENESS_CENTRALITY = "closeness_centrality";
    public static final String BETWEENNESS_CENTRALITY = "betweenness_centrality";
    public static final String EDGE_BETWEENNESS = "edge_betweenness";
    public static final String COLLABORATIVE_FILTERING = "collaborative_filtering";
    public static final String SINGLESOURCESHORTEST_PATH="singleSourceShortest";

    public static JavaSparkContext javaSparkContext = null;

    public static void processEdgeList(ProcessorMessage processorMessage) throws IOException, URISyntaxException,InterruptedException, TimeoutException {
        if(javaSparkContext == null) {
            initializeSparkContext();
        }

        Iterable<String> results = new ArrayList<>();

        // Routing
        switch (processorMessage.getAnalysis()) {
            case PAGERANK:
                // Route to PageRank
                results = Algorithms.pageRank(javaSparkContext.sc(), processorMessage.getPath());
                break;
            case CONNECTED_COMPONENTS:
                // Route to ConnectedComponents
                results = Algorithms.connectedComponents(javaSparkContext.sc(), processorMessage.getPath());
                break;
            case TRIANGLE_COUNT:
                // Route to TriangleCount
                results = Algorithms.triangleCount(javaSparkContext.sc(), processorMessage.getPath());
                break;
            case STRONGLY_CONNECTED_COMPONENTS:
                // Route to StronglyConnectedComponents
                results = Algorithms.stronglyConnectedComponents(javaSparkContext.sc(), processorMessage.getPath());
                break;
            case CLOSENESS_CENTRALITY:
                // Route to ClosenessCentrality
                results = Algorithms.closenessCentrality(javaSparkContext.sc(), processorMessage.getPath());
                break;
            case BETWEENNESS_CENTRALITY:
                // Route to BetweennessCentrality
                results = Algorithms.betweennessCentrality(javaSparkContext.sc(), processorMessage.getPath());
                break;
            case EDGE_BETWEENNESS:
                // Route to BetweennessCentrality
                results = Algorithms.edgeBetweenness(javaSparkContext.sc(), processorMessage.getPath());
                break;
            case COLLABORATIVE_FILTERING:
                results = Algorithms.collaborativeFiltering(javaSparkContext.sc(), processorMessage.getPath());
                break;
//            case SINGLESOURCESHORTEST_PATH:
//                results = JavaConversions.asJavaIterable(Algorithms.singleSourceShortestPath(javaSparkContext.sc(), processorMessage.getPath()));
//                break;
            default:
                // Analysis does not exist
                System.out.println("Did not recognize analysis key: " + processorMessage.getAnalysis());
        }


        if(processorMessage.getMode() == ProcessorMode.Partitioned) {
            processorMessage.setPath(ConfigurationLoader.getInstance().getHadoopHdfsUri() + PARTITIONED_PROPERTY_GRAPH_UPDATE_PATH.replace("{job_name}", processorMessage.getPartitionDescription().getPartitionId().toString()));
        } else {
            // Set the output path
            processorMessage.setPath(ConfigurationLoader.getInstance().getHadoopHdfsUri() + PROPERTY_GRAPH_UPDATE_PATH);
        }

        // Write results to HDFS
        com.lakala.datacenter.core.hdfs.FileUtil.writePropertyGraphUpdate(processorMessage, results);
    }

    public static JavaSparkContext initializeSparkContext() {
        SparkConf conf = new SparkConf().setAppName(ConfigurationLoader.getInstance().getAppName())
                .setMaster(ConfigurationLoader.getInstance().getSparkHost())
                .set("spark.executor.memory", "4g")
                .set("spark.driver.memory", "4g")
                .set("spark.executor.instances", "2");

        javaSparkContext = new JavaSparkContext(conf);
        return javaSparkContext;
    }
}
