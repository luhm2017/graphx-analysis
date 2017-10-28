//import java.util
//import java.util.Map
//
//import com.lakala.datacenter.constant.StreamingConstant
//import com.lakala.datacenter.utils.UtilsTools.properties
//import org.apache.commons.lang3.StringUtils.trim
//import org.neo4j.rest.graphdb.RestAPIFacade
//import org.neo4j.rest.graphdb.batch.CypherResult
//import org.neo4j.rest.graphdb.query.RestCypherQueryEngine
//import org.neo4j.rest.graphdb.util.QueryResult
//
///**
//  * Created by Administrator on 2017/7/12 0012.
//  */
//object TestApiNeo4j {
//  def main(args: Array[String]): Unit = {
////    val properies = properties(StreamingConstant.CONFIG)
////    val restAPI = new RestAPIFacade(trim(properies.getProperty(StreamingConstant.NEOIP)), trim(properies.getProperty(StreamingConstant.USER)), trim(properies.getProperty(StreamingConstant.PASSWORD)))
//
//        import scala.collection.JavaConversions._
//    //
//    //    //
//    //    val orderno ="AX20160722090751068917"
//    //    val centro = "500227198611307710"
//    //    val applyNodeIndexs = restAPI.getNodesByLabelAndProperty("" + Labels.ApplyInfo, StreamingConstant.ORDERNO, orderno)
//    //
//    //    val apply = applyNodeIndexs.toList
//    //    if (apply.size == 0) {
//    //      val applyNode = restAPI.createNode(MapUtil.map(StreamingConstant.ORDERNO, orderno.toUpperCase,StreamingConstant.MODELNAME, Labels.ApplyInfo))
//    //      applyNode.addLabel(Labels.ApplyInfo)
//    //      applyNode.setProperty(StreamingConstant.ORDERNO,orderno)
//    //
//    //      val contentIndexs = restAPI.getNodesByLabelAndProperty("Identification", StreamingConstant.CONTENT, centro)
//    //      val list = contentIndexs.toList
//    //      println(list.size)
//    //      var otherNode: RestNode = if (list.size == 0) {
//    //        val otherNode2 = restAPI.createNode(MapUtil.map(StreamingConstant.MODELNAME, "Identification", StreamingConstant.CONTENT, centro))
//    //        otherNode2.setProperty(StreamingConstant.CONTENT, centro)
//    //        otherNode2.addLabel(Labels.Identification)
//    //        otherNode2
//    //      } else {
//    //        applyNode.setProperty("cert_no", centro)
//    //        list.get(0)
//    //      }
//    //
//    //      applyNode.createRelationshipTo(otherNode, RelationshipTypes.identification)
//    //      println(otherNode.getId)
//    //      println(applyNode.getId)
//    //    }
//    val restAPI = new RestAPIFacade(trim("http://192.168.0.33:7474/db/data"), trim("neo4j"), trim("123456"))
//    val result = restAPI.query("MATCH (:Person {name:'Keanu'})-[:ACTED_IN]->(:Movie {title:'Matrix'}) RETURN count(*) as c" ,null)
//    val it = result.getData
//    it.flatten.toList.get(0)
//    println(it.flatten.toList.get(0))
//  }
//}
