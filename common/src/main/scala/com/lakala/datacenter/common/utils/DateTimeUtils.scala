package com.lakala.datacenter.common.utils

/**
  * Created by Administrator on 2017/5/5 0005.
  */
import java.util.Calendar

import org.joda.time.{DateTime, DateTimeZone, _}
import org.joda.time.format.{DateTimeFormat, ISODateTimeFormat, _}

object DateTimeUtils {

  val dateTimeZone = DateTimeZone.forID("Asia/Shanghai")

  val dateFormatter = DateTimeFormat.forPattern("yyyy-MM-dd").withZone(dateTimeZone)
  val formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZone(dateTimeZone)
  val hourFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH").withZone(dateTimeZone)
  val esTimeZone = DateTimeZone.UTC
  val esFormatter = ISODateTimeFormat.dateTime().withZoneUTC()


  def parseDataString(date: String): DateTime = {
    formatter.parseDateTime(date)
  }

  def toDateTime(time: DateTime): DateTime = {
    time.withZone(dateTimeZone)
  }

  def fromDateString(date: String): DateTime = {
    dateFormatter.parseDateTime(date)
  }

  def toDateString(time: DateTime): String = {
    dateFormatter.print(time.getMillis)
  }

  def toDateString(millis: Long): String = {
    dateFormatter.print(millis)
  }
  def toDateyMdHmsString(millis: Long): String = {
    formatter.print(millis)
  }

  def toEsTime(time: DateTime): DateTime = {
    time.withZone(esTimeZone)
  }

  def fromEsTimeString(esTimeString: String): DateTime = {
    esFormatter.parseDateTime(esTimeString)
  }

  def toEsTimeString(time: DateTime): String = {
    esFormatter.print(time.getMillis)
  }

  def toEsTimeString(millis: Long): String = {
    esFormatter.print(millis)
  }

  def toHourDateString(time: Long): String = {
    hourFormatter.print(time)
  }

  def fromDateTimeToMonthLastDay(date: String): DateTime = {
    val calendar = Calendar.getInstance();
    calendar.setTimeInMillis(DateTimeUtils.fromDateString(date).getMillis)
    calendar.set(Calendar.DATE, calendar.getActualMaximum(Calendar.DATE))
    fromDateString(toDateString(calendar.getTimeInMillis))
  }

  def fromDateTimeToMonthFirstDay(date: String): DateTime = {
    val calendar = Calendar.getInstance();
    calendar.setTimeInMillis(DateTimeUtils.fromDateString(date).getMillis)
    //设置日历中月份的第1天
    calendar.set(Calendar.DAY_OF_MONTH, 1);
    fromDateString(toDateString(calendar.getTimeInMillis))
  }

  def parseMillis(duration: String): Int = {
    try {
      var millis: Int = 0
      if (duration.endsWith("S")) {
        millis = Integer.valueOf(duration.substring(0, duration.length - 1))
      }
      else if (duration.endsWith("ms")) {
        millis = Integer.valueOf(duration.substring(0, duration.length - "ms".length))
      }
      else if (duration.endsWith("s")) {
        millis = Integer.valueOf(duration.substring(0, duration.length - 1)) * 1000
      }
      else if (duration.endsWith("m")) {
        millis = Integer.valueOf(duration.substring(0, duration.length - 1)) * 60 * 1000
      }
      else if (duration.endsWith("H") || duration.endsWith("h")) {
        millis = Integer.valueOf(duration.substring(0, duration.length - 1)) * 60 * 60 * 1000
      }
      else if (duration.endsWith("d")) {
        millis = Integer.valueOf(duration.substring(0, duration.length - 1)) * 24 * 60 * 60 * 1000
      }
      else if (duration.endsWith("w")) {
        millis = Integer.valueOf(duration.substring(0, duration.length - 1)) * 7 * 24 * 60 * 60 * 1000
      }
      else {
        millis = Integer.valueOf(duration)
      }
      millis
    }
    catch {
      case e: NumberFormatException => {
        throw new IllegalArgumentException("Failed to parse \"" + duration + "\" to millis", e)
      }
    }
  }


}
