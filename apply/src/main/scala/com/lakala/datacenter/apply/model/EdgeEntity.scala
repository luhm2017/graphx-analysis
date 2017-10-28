package com.lakala.datacenter.apply.model

/**
  * Created by ASUS-PC on 2017/4/17.
  */
class EdgeEntity(var scrId: Long, val destId: Long, var attr: String) extends Serializable with Product {
  override def productElement(idx: Int): Any = idx match {
    case 0 => scrId
    case 1 => destId
    case 2 => attr
  }

  override def productArity: Int = 3

  override def canEqual(that: Any): Boolean = that.isInstanceOf[EdgeEntity]

  override def equals(other: Any): Boolean = other match {
    case that: EdgeEntity =>
      (that canEqual this) &&
        scrId == that.scrId &&
        destId == that.destId
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(scrId, destId)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}
