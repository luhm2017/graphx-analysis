import com.lakala.datacenter.common.utils.DateTimeUtils
import com.lakala.datacenter.constant.StreamingConstant
import org.apache.commons.lang3.StringUtils
import org.joda.time.DateTime
import org.neo4j.driver.v1._

/**
  * Created by Administrator on 2017/8/2 0002.
  */
object TestCypher2 {
  val driver: Driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "123456"))

  def main(args: Array[String]): Unit = {
    var map: java.util.HashMap[String, String] = new java.util.HashMap[String, String]()
    var paramMap: java.util.HashMap[String, String] = new java.util.HashMap[String, String]()
    map.put("orderno", "TNA20170623102711010234032084429")
    map.put("_DeviceId", "A000005966DFEA")
    map.put("mobile", "18961922790")

    runCypherApply(driver.session(), map)
    driver.close()
  }

  private def runCypherApply(session: Session, map: java.util.HashMap[String, String]): Unit = {
    val applyStatementTemplate = new StringBuffer("MERGE (apply:ApplyInfo {orderno:$orderno})")
    applyStatementTemplate.append(" ON MATCH SET apply.modelname='ApplyInfo',apply.insertTime=$insertTime,apply.user_id=$user_id")
    val otherStatementTemplate = new StringBuffer()
    val relStatementTemplate = new StringBuffer()

    var paramMap: java.util.HashMap[String, Object] = new java.util.HashMap[String, Object]()
    paramMap.put("orderno", map.getOrDefault(StreamingConstant.ORDERNO, ""))
    paramMap.put(StreamingConstant.INSERTTIME, DateTimeUtils.formatter.print(DateTime.now()))
    paramMap.put(StreamingConstant.USER_ID, map.getOrDefault(StreamingConstant.USERID, ""))

    for (key <- StreamingConstant.fieldMap.keySet) {
      val fieldRelation = StreamingConstant.fieldMap.get(key).get.split(",")
      if (StringUtils.isNoneEmpty(map.get(key))) {
        val modelname = "" + StreamingConstant.labelMap.get(key).get
        val rel = "" + StreamingConstant.relationShipMap.get(key).get
        otherStatementTemplate.append(" MERGE (" + key + ":" + modelname + "{modelname:'" + modelname + "',content:$" + fieldRelation(0) + "})")
        otherStatementTemplate.append(" MERGE (apply)-[:" + rel + "]->(" + key + ")")
        applyStatementTemplate.append(",apply." + fieldRelation(0) + "=$" + fieldRelation(0))
        paramMap.put(fieldRelation(0), map.get(key))
      }
    }

    val statementStr = applyStatementTemplate.append(otherStatementTemplate).toString
    println(statementStr)
    session.writeTransaction(new TransactionWork[Integer]() {
      override def execute(tx: Transaction): Integer = {
        tx.run(statementStr, paramMap)
        1
      }
    })
  }
}
