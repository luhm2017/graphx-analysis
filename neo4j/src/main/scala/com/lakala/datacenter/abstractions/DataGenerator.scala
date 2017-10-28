package com.lakala.datacenter.abstractions

import com.lakala.datacenter.utils.Config

/**
  * Created by Administrator on 2017/5/31 0031.
  */
trait DataGenerator {
  def generateUsers(config: Config): Unit
}
