package com.lakala.datacenter.apply.model

/**
  * Created by ASUS-PC on 2017/4/18.
  */
class CallHistoryEntity(var loginname: Long = 0L, var caller_phone: Long = 0L) extends BaseEntity with Serializable with Product {
  override def productElement(idx: Int): Any = idx match {
    case 0 => loginname
    case 1 => caller_phone

  }

  override def productArity: Int = 2

  override def canEqual(that: Any): Boolean = that.isInstanceOf[CallHistoryEntity]

  override def equals(other: Any): Boolean = other match {
    case that: CallHistoryEntity =>
      (that canEqual this) &&
        loginname == that.loginname &&
        caller_phone == that.caller_phone
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(loginname, caller_phone)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString = s"CallHistoryEntity($loginname, $caller_phone)"
}
