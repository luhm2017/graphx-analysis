package com.lakala.datacenter.utils

import java.io.Serializable
import java.util.Properties

import org.slf4j.LoggerFactory

/**
  * Created by lenovo on 2016/8/10.
  */
object UtilsTools {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def properties(propertiesPath: String): Properties = {
    var _properties: Option[Properties] = None
    _properties match {
      case None => {
        logger.info("Loading configuration...")
        val inputStream = this.getClass.getClassLoader.getResourceAsStream(propertiesPath)
        val underlying = new Properties()
        underlying.load(inputStream)
        _properties = Some(underlying)
        underlying
      }
      case Some(underlying) => {
        underlying
      }
    }
    _properties.get
  }


}
