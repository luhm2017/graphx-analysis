//package org.neo4j.spark
//
//import java.io.File
//
//import com.lakala.datacenter.load.spark.{Neo4jDataFrame, Neo4jGraph}
//import org.apache.commons.lang3.StringUtils.trim
//import org.apache.spark.api.java.JavaSparkContext
//import org.apache.spark.graphx.{Edge, Graph}
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
//import org.apache.spark.sql.{Row, SQLContext}
//import org.apache.spark.{SparkConf, SparkContext}
//import org.junit.Assert._
//import org.junit._
//import org.neo4j.harness.{ServerControls, TestServerBuilders}
//import org.neo4j.rest.graphdb.RestAPIFacade
//import org.neo4j.rest.graphdb.batch.CypherResult
//
//
///**
//  * @author lys
//  * @since 17.07.16
//  */
//class Neo4jDataFrameScalaTest {
//  val FIXTURE: String = "CREATE (:A)-[:REL {foo:'bar'}]->(:B)"
//  private var conf: SparkConf = null
//  private var sc: JavaSparkContext = null
//  private var server: ServerControls = null
//  private val path:String ="F:\\tmp\\neo4j\\tmp02"
//  private var restAPI:RestAPIFacade = null
//  @Before
//  @throws[Exception]
//  def setUp {
////    server = TestServerBuilders.newInProcessBuilder(new File(path)).withConfig("dbms.security.auth_enabled", "false").withFixture(FIXTURE).newServer
//   restAPI = new RestAPIFacade(trim(Neo4jContstanTest.RESTNEO4JURL), trim("neo4j"), trim("123456"))
//
//    conf = new SparkConf().setAppName("neoTest").setMaster("local[*]").set("spark.driver.allowMultipleContexts", "true").set("spark.neo4j.bolt.url", Neo4jContstanTest.SERVER_BOLTURI)
//    sc = SparkContext.getOrCreate(conf)
//  }
//
//  @After def tearDown {
////    server.close
//    sc.close
//  }
//
//  @Test def mergeEdgeList {
//    val rows = sc.makeRDD(Seq(Row("Keanu", "Matrix")))
//    val schema = StructType(Seq(StructField("name", DataTypes.StringType), StructField("title", DataTypes.StringType)))
//    val sqlContext = new SQLContext(sc)
//    val df = sqlContext.createDataFrame(rows, schema)
//    Neo4jDataFrame.mergeEdgeList(sc, df, ("Person", Seq("name")), ("ACTED_IN", Seq.empty), ("Movie", Seq("title")))
//    val edges: RDD[Edge[Long]] = sc.makeRDD(Seq(Edge(0, 1, 42L)))
//    val graph = Graph.fromEdges(edges, -1)
//    assertEquals(2, graph.vertices.count)
//    assertEquals(1, graph.edges.count)
//    Neo4jGraph.saveGraph(sc, graph, null, "test")
//
////    val it: ResourceIterator[Long] = server.graph().execute("MATCH (:Person {name:'Keanu'})-[:ACTED_IN]->(:Movie {title:'Matrix'}) RETURN count(*) as c").columnAs("c")
//    val result: CypherResult = restAPI.query("MATCH (:Person {name:'Keanu'})-[:ACTED_IN]->(:Movie {title:'Matrix'}) RETURN count(*) as c" ,null)
//    import scala.collection.JavaConversions._
//    assertEquals(1L, result.getData.flatten.toList.get(0).toString.toLong)
//    restAPI.close()
//  }
//}
//
