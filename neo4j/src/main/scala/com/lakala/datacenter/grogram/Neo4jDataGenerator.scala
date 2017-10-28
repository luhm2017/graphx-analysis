package com.lakala.datacenter.grogram

import java.io.File

import com.google.common.base.Splitter
import com.lakala.datacenter.abstractions.DataGenerator
import com.lakala.datacenter.enums.{Labels, RelationshipTypes}
import com.lakala.datacenter.utils.Config
import org.apache.commons.lang3.StringUtils
import org.neo4j.graphdb._
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.io.Source

/**
  * Created by Administrator on 2017/5/31 0031.
  */
class Neo4jDataGenerator extends DataGenerator {
  private val log = LoggerFactory.getLogger("Neo4jDataGenerator")
  val BATCH_SIZE: Int = 1000

  private var graphdb: GraphDatabaseService = null
  private val nodeMap: mutable.HashMap[String, Node] with Object = new scala.collection.mutable.HashMap[String, Node]()

  def this(graphdb: GraphDatabaseService) {
    this()
    this.graphdb = graphdb
  }

  override def generateUsers(config: Config): Unit = {
    var tx = graphdb.beginTx
    try {
      var i: Int = 0
      for (file <- subdirs(new File(config.input))) {
        //orderId,contractNo,termId,loanPan,returnPan,insertTime,recommend,userId,
        // deviceId
        //certNo,email,company,mobile,compAddr,compPhone,emergencyContactMobile,contactMobile,ipv4,msgphone,telecode
        for (line <- Source.fromFile(file, "UTF-8").getLines()) {
          i += 1
          import scala.collection.JavaConversions._
          val list = Splitter.on(",").trimResults().split(line).toList
          try {
            createNodeSetProperty(graphdb, list)
          } catch {
            case e: Exception => {
              println("curent line" + i + " content:" + line + " \n" +
                "exception:" + e.getMessage + " \n" +
                "exception3:" + e.getStackTraceString)
            }
          }

          //          nodeMap.add(user.getId)
          if (i % BATCH_SIZE == 0) {
            println("Commited batch:" + i)
            tx.success()
            tx.close()
            tx = graphdb.beginTx
            log.info("Commited batch")
          }
        }
      }

      log.info("Commited batch")
      println("Commited batch: " + i)
      tx.success()
      tx.close()
      createSchemaIndex()
      Thread.sleep(500)
    } catch {
      case e: Exception => {
        println("exception:" + e.getMessage + "\n" +
          "exception3:" + e.getStackTraceString)
        tx.failure()
      }
    } finally {
      nodeMap.clear()
      tx.close()
    }

  }

  def createSchemaIndex(): Unit = {
    var tx = graphdb.beginTx
    graphdb.schema().indexFor(Labels.ApplyInfo).on("orderno").create()
    graphdb.schema().indexFor(Labels.Terminal).on("content").create()
    graphdb.schema().indexFor(Labels.BankCard).on("content").create()
    graphdb.schema().indexFor(Labels.Mobile).on("content").create()
    graphdb.schema().indexFor(Labels.Device).on("content").create()
    graphdb.schema().indexFor(Labels.Identification).on("content").create()
    graphdb.schema().indexFor(Labels.Email).on("content").create()
    graphdb.schema().indexFor(Labels.Company).on("content").create()
    graphdb.schema().indexFor(Labels.CompanyAddress).on("content").create()
    graphdb.schema().indexFor(Labels.CompanyTel).on("content").create()
    graphdb.schema().indexFor(Labels.IPV4).on("content").create()
    tx.success()
  }

  def subdirs(dir: File): Iterator[File] = {
    val dirs = dir.listFiles().filter(_.isDirectory())
    val files = dir.listFiles().filter(_.isFile())
    files.toIterator ++ dirs.toIterator.flatMap(subdirs _)
  }

  def createNodeSetProperty(graphdb: GraphDatabaseService, list: java.util.List[String]): Unit = {
    val apply = graphdb.createNode(Labels.ApplyInfo)
    apply.setProperty("orderno", list.get(0))
    apply.setProperty("modelname", "" + Labels.ApplyInfo)
    //    graphdb.index().forNodes("orderno").add(apply,"modelname", ""+Labels.ApplyInfo)
    if (StringUtils.isNoneEmpty(list.get(1))) {
      apply.setProperty("contract_no", list.get(1))
    }

    if (StringUtils.isNoneEmpty(list.get(2))) {
      apply.setProperty("term_id", list.get(2))
      var terminal: Option[Node] = nodeMap.get(list.get(2))
      if (!terminal.isDefined) {
        val tmpNode = graphdb.createNode(Labels.Terminal)
        tmpNode.setProperty("modelname", "" + Labels.Terminal)
        //        graphdb.index().forNodes("term_id").add(tmpNode, "modelname", "" + Labels.Terminal)
        tmpNode.setProperty("content", list.get(2))
        nodeMap.put(list.get(2), tmpNode)
        terminal = nodeMap.get(list.get(2))
      }
      apply.createRelationshipTo(terminal.get, RelationshipTypes.terminal)
    }

    if (StringUtils.isNoneEmpty(list.get(3))) {
      apply.setProperty("loan_pan", list.get(3))
      var bankcard: Option[Node] = nodeMap.get(list.get(3))
      if (!bankcard.isDefined) {
        val tmpNode = graphdb.createNode(Labels.BankCard)
        tmpNode.setProperty("modelname", "" + Labels.BankCard)
        //        graphdb.index().forNodes("loan_pan").add(tmpNode, "modelname", "" + Labels.BankCard)
        tmpNode.setProperty("content", list.get(3))
        nodeMap.put(list.get(3), tmpNode)
        bankcard = nodeMap.get(list.get(3))
      }
      apply.createRelationshipTo(bankcard.get, RelationshipTypes.bankcard)
    }

    if (StringUtils.isNoneEmpty(list.get(4))) {
      apply.setProperty("return_pan", list.get(4))
      var bankcard: Option[Node] = nodeMap.get(list.get(4))
      if (!bankcard.isDefined) {
        val tmpNode = graphdb.createNode(Labels.BankCard)
        tmpNode.setProperty("modelname", "" + Labels.BankCard)
        //        graphdb.index().forNodes("return_pan").add(tmpNode, "modelname", "" + Labels.BankCard)

        tmpNode.setProperty("content", list.get(4))
        nodeMap.put(list.get(4), tmpNode)
        bankcard = nodeMap.get(list.get(4))
      }
      apply.createRelationshipTo(bankcard.get, RelationshipTypes.bankcard)
    }
    if (list.size() > 5 && StringUtils.isNoneEmpty(list.get(5))) {
      apply.setProperty("insert_time", list.get(5))
    }

    if (list.size() > 6 && StringUtils.isNoneEmpty(list.get(6)) && (list.get(6).length == 6 || list.get(6).length == 11)) {
      apply.setProperty("recommend", list.get(6))
      var recommend: Option[Node] = nodeMap.get(list.get(6))
      if (!recommend.isDefined) {
        val tmpNode = graphdb.createNode(Labels.Mobile)
        tmpNode.setProperty("modelname", "" + Labels.Mobile)
        //        graphdb.index().forNodes("recommend").add(tmpNode, "modelname", "" + Labels.Mobile)

        tmpNode.setProperty("content", list.get(6))
        nodeMap.put(list.get(6), tmpNode)
        recommend = nodeMap.get(list.get(6))
      }
      apply.createRelationshipTo(recommend.get, RelationshipTypes.recommend)
    }
    if (list.size() > 7 && StringUtils.isNoneEmpty(list.get(7))) {
      apply.setProperty("user_id", list.get(7))
    }

    if (list.size() > 8 && StringUtils.isNoneEmpty(list.get(8)) && !"00000000-0000-0000-0000-000000000000".equals(list.get(8))) {
      apply.setProperty("device_id", list.get(8))

      var device: Option[Node] = nodeMap.get(list.get(8))
      if (!device.isDefined) {
        val tmpNode = graphdb.createNode(Labels.Device)
        tmpNode.setProperty("modelname", "" + Labels.Device)
        //        graphdb.index().forNodes("device_id").add(tmpNode,"modelname", ""+Labels.Device)

        tmpNode.setProperty("content", list.get(8))
        nodeMap.put(list.get(8), tmpNode)
        device = nodeMap.get(list.get(8))
      }
      apply.createRelationshipTo(device.get, RelationshipTypes.device)
    }

    if (list.size() > 9 && StringUtils.isNoneEmpty(list.get(9))) {
      apply.setProperty("cert_no", list.get(9))
      var identification: Option[Node] = nodeMap.get(list.get(9))
      if (!identification.isDefined) {
        val tmpNode = graphdb.createNode(Labels.Identification)
        tmpNode.setProperty("modelname", "" + Labels.Identification)
        //        graphdb.index().forNodes("cert_no").add(tmpNode,"modelname", ""+Labels.Identification)

        tmpNode.setProperty("content", list.get(9))
        nodeMap.put(list.get(9), tmpNode)
        identification = nodeMap.get(list.get(9))
      }
      apply.createRelationshipTo(identification.get, RelationshipTypes.identification)
    }
    if (list.size() > 10 && StringUtils.isNoneEmpty(list.get(10))) {
      apply.setProperty("email", list.get(10).toLowerCase)
      var email: Option[Node] = nodeMap.get(list.get(10).toLowerCase)
      if (!email.isDefined) {
        val tmpNode = graphdb.createNode(Labels.Email)
        tmpNode.setProperty("modelname", "" + Labels.Email)
        //        graphdb.index().forNodes("email").add(tmpNode,"modelname", ""+Labels.Email)

        tmpNode.setProperty("content", list.get(10).toLowerCase)
        nodeMap.put(list.get(10).toLowerCase, tmpNode)
        email = nodeMap.get(list.get(10).toLowerCase)
      }
      apply.createRelationshipTo(email.get, RelationshipTypes.email)
    }
    if (list.size() > 11 && StringUtils.isNoneEmpty(list.get(11))) {
      apply.setProperty("company", list.get(11))
      var company: Option[Node] = nodeMap.get(list.get(11))
      if (!company.isDefined) {
        val tmpNode = graphdb.createNode(Labels.Company)
        tmpNode.setProperty("modelname", "" + Labels.Company)
        //        graphdb.index().forNodes("company").add(tmpNode,"modelname", ""+Labels.Company)

        tmpNode.setProperty("content", list.get(11))
        nodeMap.put(list.get(11), tmpNode)
        company = nodeMap.get(list.get(11))
      }
      apply.createRelationshipTo(company.get, RelationshipTypes.company)
    }

    if (list.size() > 12 && StringUtils.isNoneEmpty(list.get(12))) {
      apply.setProperty("mobile", list.get(12))
      var loginmobile: Option[Node] = nodeMap.get(list.get(12))
      if (!loginmobile.isDefined) {
        val tmpNode = graphdb.createNode(Labels.Mobile)
        tmpNode.setProperty("modelname", "" + Labels.Mobile)
        tmpNode.setProperty("content", list.get(12))
        nodeMap.put(list.get(12), tmpNode)
        loginmobile = nodeMap.get(list.get(12))
      }
      apply.createRelationshipTo(loginmobile.get, RelationshipTypes.loginmobile)
    }

    if (list.size() > 13 && StringUtils.isNoneEmpty(list.get(13))) {
      apply.setProperty("comp_addr", list.get(13))
      var companyaddress: Option[Node] = nodeMap.get(list.get(13))
      if (!companyaddress.isDefined) {
        val tmpNode = graphdb.createNode(Labels.CompanyAddress)
        tmpNode.setProperty("modelname", "" + Labels.CompanyAddress)
        //        graphdb.index().forNodes("comp_addr").add(tmpNode,"modelname", ""+Labels.CompanyAddress)

        tmpNode.setProperty("content", list.get(13))
        nodeMap.put(list.get(13), tmpNode)
        companyaddress = nodeMap.get(list.get(13))
      }
      apply.createRelationshipTo(companyaddress.get, RelationshipTypes.companyaddress)
    }

    if (list.size() > 14 && StringUtils.isNoneEmpty(list.get(14))) {
      apply.setProperty("comp_phone", list.get(14))
      var companytel: Option[Node] = nodeMap.get(list.get(14))
      if (!companytel.isDefined) {
        val tmpNode = graphdb.createNode(Labels.CompanyTel)
        tmpNode.setProperty("modelname", "" + Labels.CompanyTel)
        //        graphdb.index().forNodes("comp_phone").add(tmpNode,"modelname", ""+Labels.CompanyTel)

        tmpNode.setProperty("content", list.get(14))
        nodeMap.put(list.get(14), tmpNode)
        companytel = nodeMap.get(list.get(14))
      }
      apply.createRelationshipTo(companytel.get, RelationshipTypes.companytel)
    }

    if (list.size() > 15 && StringUtils.isNoneEmpty(list.get(15))) {
      apply.setProperty("emergencymobile", list.get(15))
      var emergencymobile: Option[Node] = nodeMap.get(list.get(15))
      if (!emergencymobile.isDefined) {
        val tmpNode = graphdb.createNode(Labels.Mobile)
        tmpNode.setProperty("modelname", "" + Labels.Mobile)
        //        graphdb.index().forNodes("emergencymobile").add(tmpNode,"modelname", ""+Labels.Mobile)

        tmpNode.setProperty("content", list.get(15))
        nodeMap.put(list.get(15), tmpNode)
        emergencymobile = nodeMap.get(list.get(15))
      }
      apply.createRelationshipTo(emergencymobile.get, RelationshipTypes.emergencymobile)
    }
    if (list.size() > 16 && StringUtils.isNoneEmpty(list.get(16))) {
      apply.setProperty("contact_mobile", list.get(16))
      var relativecontact: Option[Node] = nodeMap.get(list.get(16))
      if (!relativecontact.isDefined) {
        val tmpNode = graphdb.createNode(Labels.Mobile)
        tmpNode.setProperty("modelname", "" + Labels.Mobile)
        //        graphdb.index().forNodes("contact_mobile").add(tmpNode,"modelname", ""+Labels.Mobile)

        tmpNode.setProperty("content", list.get(16))
        nodeMap.put(list.get(16), tmpNode)
        relativecontact = nodeMap.get(list.get(16))
      }
      apply.createRelationshipTo(relativecontact.get, RelationshipTypes.relativecontact)
    }

    if (list.size() > 17 && StringUtils.isNoneEmpty(list.get(17))) {
      apply.setProperty("ipv4", list.get(17))
      var relativecontact: Option[Node] = nodeMap.get(list.get(17))
      if (!relativecontact.isDefined) {
        val tmpNode = graphdb.createNode(Labels.IPV4)
        tmpNode.setProperty("modelname", "" + Labels.IPV4)
        //        graphdb.index().forNodes("ipv4").add(tmpNode,"modelname", ""+Labels.IPV4)

        tmpNode.setProperty("content", list.get(17))
        nodeMap.put(list.get(17), tmpNode)
        relativecontact = nodeMap.get(list.get(17))
      }
      apply.createRelationshipTo(relativecontact.get, RelationshipTypes.ipv4)
    }

    if (list.size() > 18 && StringUtils.isNoneEmpty(list.get(18))) {
      apply.setProperty("msgphone", list.get(18))
      var relativecontact: Option[Node] = nodeMap.get(list.get(18))
      if (!relativecontact.isDefined) {
        val tmpNode = graphdb.createNode(Labels.Mobile)
        tmpNode.setProperty("modelname", "" + Labels.Mobile)
        //        graphdb.index().forNodes("msgphone").add(tmpNode,"modelname", ""+Labels.Mobile)

        tmpNode.setProperty("content", list.get(18))
        nodeMap.put(list.get(18), tmpNode)
        relativecontact = nodeMap.get(list.get(18))
      }
      apply.createRelationshipTo(relativecontact.get, RelationshipTypes.applymymobile)
    }

    if (list.size() > 19 && StringUtils.isNoneEmpty(list.get(19))) {
      apply.setProperty("hometel", list.get(19))
      var relativecontact: Option[Node] = nodeMap.get(list.get(19))
      if (!relativecontact.isDefined) {
        val tmpNode = graphdb.createNode(Labels.Mobile)
        tmpNode.setProperty("modelname", "" + Labels.Mobile)
        //        graphdb.index().forNodes("hometel").add(tmpNode,"modelname", ""+Labels.Mobile)

        tmpNode.setProperty("content", list.get(19))
        nodeMap.put(list.get(19), tmpNode)
        relativecontact = nodeMap.get(list.get(19))
      }
      apply.createRelationshipTo(relativecontact.get, RelationshipTypes.hometel)
    }
  }

}
