/**
 * @author Ting Pan <tpan35@gatech.edu>.
 */
package edu.gatech.cse8803.ioutils

import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.SQLContext
import com.databricks.spark.csv.CsvContext


object CSVUtils {
  def loadCSVAsTable(sqlContext: SQLContext, path: String, tableName: String): SchemaRDD = {
    val data = sqlContext.csvFile(path)
    data.registerTempTable(tableName)
    data
  }

  def loadCSVAsTable(sqlContext: SQLContext, path: String): SchemaRDD = {
    loadCSVAsTable(sqlContext, path, inferTableNameFromPath(path))
  }

  private val pattern = "(\\w+)(\\.csv)?$".r.unanchored
  def inferTableNameFromPath(path: String) = path match {
    case pattern(filename, extension) => filename
    case _ => path
  }
}
