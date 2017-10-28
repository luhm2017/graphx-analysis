package entity

import scala.collection.mutable.ListBuffer

/**
  * Created by ASUS-PC on 2017/4/19.
  */
case class CallEntity(var totalRounds: Int = 0, var propertyList: ListBuffer[String] = ListBuffer()) extends Serializable with Product {
  override def productElement(idx: Int): Any = idx match {
    case 0 => totalRounds
    case 1 => propertyList
  }

  override def productArity: Int = 2

  override def canEqual(that: Any): Boolean = that.isInstanceOf[CallEntity]

  override def toString = s"CallEntity($totalRounds, ${propertyList.toArray.mkString(",")})"
}
