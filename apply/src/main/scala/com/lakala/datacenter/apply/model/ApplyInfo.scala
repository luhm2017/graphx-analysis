package com.lakala.datacenter.apply.model

import org.apache.commons.lang3.StringUtils

/**
  * Created by ASUS-PC on 2017/4/13.
  */
class ApplyInfo(var order_id: String = "",
                var contract_no: String = "",
                var business_no: String = "",
                var term_id: String = "",
                var loan_pan: String = "",
                var return_pan: String = "",
                var empmobile: String = "",
                var datatype: Int = 0 //0,1黑,2百
               ) extends BaseEntity with Product {
  override def toString = s"ApplyInfo(order_id=$order_id, contract_no=$contract_no, business_no=$business_no, term_id=$term_id, loan_pan=$loan_pan, return_pan=$return_pan, empmobile=$empmobile)"

  override def productElement(idx: Int): Any = idx match {
    case 0 => order_id
    case 1 => contract_no
    case 2 => business_no
    case 3 => term_id
    case 4 => loan_pan
    case 5 => return_pan
    case 6 => empmobile
    case 7 => datatype
    case 8 => inDeg
    case 9 => outDeg
  }

  override def productArity: Int = 10

  override def canEqual(that: Any): Boolean = that.isInstanceOf[ApplyInfo]

  override def equals(other: Any): Boolean = other match {
    case that: ApplyInfo =>
      (that canEqual this) &&
        order_id == that.order_id &&
        contract_no == that.contract_no &&
        business_no == that.business_no &&
        term_id == that.term_id &&
        loan_pan == that.loan_pan &&
        return_pan == that.return_pan &&
        empmobile == that.empmobile
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(order_id, contract_no, business_no, term_id, loan_pan, return_pan, empmobile)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  def getKey: String = {
    if (StringUtils.isNotEmpty(order_id)) order_id
    else if (StringUtils.isNotEmpty(contract_no)) contract_no
    else if (StringUtils.isNotEmpty(business_no)) business_no
    else if (StringUtils.isNotEmpty(term_id)) term_id
    else if (StringUtils.isNotEmpty(loan_pan)) loan_pan
    else if (StringUtils.isNotEmpty(return_pan)) return_pan
    else empmobile
  }
}
