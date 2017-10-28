/**
  * Created by Administrator on 2017/8/1 0001.
  */
import org.neo4j.driver.v1.GraphDatabase
import org.neo4j.driver.v1.AuthTokens
import com.lakala.datacenter.cypher.NeoData._

object Main {

  def main(args: Array[String]): Unit = {

    val driver = GraphDatabase.driver("bolt://localhost", AuthTokens.basic("neo4j", "123456"))

    val session = driver.session();

    val nodes = allNodes(session)

    println(nodes.mkString("\n"))
  }
}