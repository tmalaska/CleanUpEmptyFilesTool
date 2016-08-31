package com.cloudera.sa.cap1.spark

import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object CountFilesInHiveTables {
  def main(args: Array[String]): Unit = {

    if (args.length == 0) {
      println("Args: <runLocal> " +
        "<inputFolder> " +
        "<inputHiveTable> " +
        "<outputFolder> " +
        "<outputTable> ")
      return
    }

    val runLocal = args(0).equalsIgnoreCase("l")
    val inputFolder = args(1)
    val inputHiveTable = args(2)
    val outputFolder = args(3)
    val outputTable = args(3)

    val sc: SparkContext = if (runLocal) {
      val sparkConfig = new SparkConf()
      sparkConfig.set("spark.broadcast.compress", "false")
      sparkConfig.set("spark.shuffle.compress", "false")
      sparkConfig.set("spark.shuffle.spill.compress", "false")
      new SparkContext("local", "TableStatsSinglePathMain", sparkConfig)
    } else {
      val sparkConfig = new SparkConf().setAppName("TableStatsSinglePathMain")
      new SparkContext(sparkConfig)
    }

    val sql = new HiveContext(sc)

    val hiveTablesRdd = sc.textFile(inputFolder).map(line => {
      var tablePath = line.split('\t')(4)
      if (tablePath.startsWith("hdfs://nameservice1")) {
        tablePath = tablePath.substring(19)
      }
      (tablePath, 1)
    })

    val topFolderTotals = sql.sql("select path_nm, blk_num from " + inputHiveTable).map(r => {
      val filePath = r.getString(0)
      val tablePath = if (filePath.contains('=')) {
        val lastEqual = filePath.lastIndexOf('=')
        val lastForwardSlash = filePath.substring(0, lastEqual).lastIndexOf('/')
        if (lastForwardSlash > 0) {
          filePath.substring(0, lastForwardSlash)
        } else {
          filePath
        }
      } else {
        val lastForwardSlash = filePath.lastIndexOf('/')
        if (lastForwardSlash > 0) {
          filePath.substring(0, lastForwardSlash)
        } else {
          filePath
        }
      }

      (tablePath, (1, r.getLong(1)))
    }).reduceByKey((a, b) => {
      (a._1 + b._1, a._2 + b._2)
    })

    val resultRdd = hiveTablesRdd.join(topFolderTotals).map(r => {
      Row(r._1, r._2._2._1, r._2._2._2)
    })

    sql.sql("create external table  IF NOT EXISTS " + outputTable + " ( " +
      " table_path string, " +
      " file_count int," +
      " block_count long," +
      " location '" + outputFolder + "/hive/" + outputTable + "'")

    val emptyDf = sql.sql("select * from " + outputTable + " limit 0")

    sql.createDataFrame(resultRdd, emptyDf.schema).registerTempTable("foobar42")

    sql.sql("INSERT OVERWRITE TABLE " + outputTable + " select * from foobar42")

    sc.stop()
  }
}
