package com.lakala.datacenter.apply.model

/**
  * Created by ASUS-PC on 2017/4/17.
  */
trait BaseEntity extends Serializable {
  var inDeg: Int = 0;
  var outDeg: Int = 0;
}
