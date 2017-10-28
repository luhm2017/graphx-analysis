package com.lakala.datacenter.core.utils

import java.nio.charset.StandardCharsets

import com.google.common.hash.Hashing
import com.lakala.datacenter.common.utils.DateTimeUtils

import scala.util.matching.Regex

/**
  * Created by ASUS-PC on 2017/4/18.
  */
object UtilsToos {
  /**
    * 根据字符串生成唯一的hashcode值
    *
    * @param str
    * @return
    */
  def hashId(str: String) = {
    Hashing.md5().hashString(str, StandardCharsets.UTF_8).asLong()
  }

  /**
    * 手机号,电话号码验证
    *
    * @param  num
    * @return 验证通过返回true
    */
  def isMobileOrPhone(num: String): Boolean = {
    val pattern = new Regex("^((17[0-9])(14[0-9])|(13[0-9])|(15[^4,\\D])|(18[0,5-9]))\\d{8}$")
    val pattern2 = new Regex("(?:(\\(\\+?86\\))(0[0-9]{2,3}\\-?)?([2-9][0-9]{6,7})+(\\-[0-9]{1,4})?)|(?:(86-?)?(0[0-9]{2,3}\\-?)?([2-9][0-9]{6,7})+(\\-[0-9]{1,4})?)") // 验证带区号的
//    val pattern2 = new Regex("^[0][1-9]{2,3}-[0-9]{5,10}$") // 验证带区号的
    val pattern3 = new Regex("^[1-9]{1}[0-9]{5,8}$") // 验证没有区号的
    num match {
      case pattern(_*) => {
        true
      }
      case pattern2(_*) => {
        true
      }
      case pattern3(_*) => {
        true
      }
      case _ => {
        false
      }
    }
  }




}
