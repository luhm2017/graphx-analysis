import com.lakala.datacenter.core.utils.UtilsToos.hashId
import org.apache.spark.graphx.Edge

import scala.collection.mutable.ListBuffer

/**
  * Created by ASUS-PC on 2017/4/12.
  */
object TestRunGraphx {
  def main(args: Array[String]): Unit = {
//    RunLoadApplyGraphx2.main(Array())
//        com.lakala.datacenter.main.Driver.main(args)
//    val s ="15397661996->XNW28459058720408576"
//    val ss = "13666199888->XNW28459058720408576"
//    s.substring(0,s.indexOf("->"))
//    println(s.substring(0,s.indexOf("->"))+"##"+ss.substring(0,ss.indexOf("->")))
    val arry= args(0).split(",")
    val edge =new EdgeArr("001","4334","7","0")
    if(judgSendMsg(arry,edge)) println("=========")
  }
  def judgSendMsg(sendType: Array[String], edge: EdgeArr): Boolean = {
    var flag = false
    for (stype <- sendType) if (edge.srcType.equals(stype)) flag = true
    flag
  }

//  var messages = g.mapReduceTriplets(sendMsg,mergeMsg);
//  print("messages:"+messages.take(10).mkString("\n"))
//  var activeMessages = messages.count();
//  //LOAD
//  var prevG:Graph[VD,ED] = null
//  var i = 0;
//  while(activeMessages > 0 && i < maxIterations){
//    //③Receive the messages.Vertices that didn‘t get any message do not appear in newVerts.
//    //内联操作，返回的结果是VertexRDD，可以参看后面的调试信息
//    val newVerts = g.vertices.innerJoin(messages)(vprog).cache();
//    print("newVerts:"+newVerts.take(10).mkString("\n"))
//    //④update the graph with the new vertices.
//    prevG = g;//先把旧的graph备份，以利于后面的graph更新和unpersist掉旧的graph
//    　　　　　//④外联操作，返回整个更新的graph
//    g = g.outerJoinVertices(newVerts){(vid,old,newOpt) => newOpt.getOrElse(old)}//getOrElse方法，意味，如果newOpt存在，返回newOpt,不存在返回old
//    print(g.vertices.take(10).mkString("\n"))
//    g.cache();//新的graph cache起来，下一次迭代使用
//
//    val oldMessages = messages;//备份，同prevG = g操作一样
//    //Send new messages.Vertices that didn‘t get any message do not appear in newVerts.so
//    //don‘t send messages.We must cache messages.so it can be materialized on the next line.
//    //allowing us to uncache the previous iteration.
//    　　　 //⑤下一次迭代要发送的新的messages,先cache起来
//    messages = g.mapReduceTriplets(sendMsg,mergeMsg,Some((newVerts,activeDirection))).cache()
//    print("下一次迭代要发送的messages:"+messages.take(10).mkString("\n"))
//    activeMessages = messages.count();//⑥
//    print("下一次迭代要发送的messages的个数："+ activeMessages)//如果activeMessages==0，迭代结束
//    logInfo("Pregel finished iteration" + i);
//    　　　 //原来，旧的message和graph不可用了，unpersist掉
//    oldMessages.unpersist(blocking= false);
//    newVerts.unpersist(blocking=false)//unpersist之后，就不可用了
//    prevG.unpersistVertices(blocking=false)
//    prevG.edges.unpersist(blocking=false)
//    i += 1;
//  }
//  g//返回最后的graph
//}
//
//}



  //  val conf = if (ctx.isLocals) new Configuration else ctx.getSparkContext.hadoopConfiguration
  //  val hdfsPath: String = hdfsMasterPath + path
  //  rdd.saveAsTextFile(hdfsPath)
  //  hiveCT.sql(s"ALTER TABLE $tableName  DROP PARTITION(execute_dt='$date', project_id='$project')")
  //  hiveCT.sql(s"ALTER TABLE $tableName SET FILEFORMAT TEXTFILE")
  //  hiveCT.sql(s"LOAD DATA INPATH '$hdfsPath/part-*' OVERWRITE INTO TABLE $tableName PARTITION (execute_dt='$date', project_id='$project')")
  //  hiveCT.sql(s"ALTER TABLE $tableName SET FILEFORMAT RCFILE")









}
