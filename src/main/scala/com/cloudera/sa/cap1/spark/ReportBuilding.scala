package com.cloudera.sa.cap1.spark

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object ReportBuilding {
  def main(args: Array[String]): Unit = {

    if (args.length == 0) {
      println("Args: <runLocal> <inputFolder> " +
        "<outputFolder> " +
        "<numberOfPartition> ")
      return
    }

    val runLocal = args(0).equalsIgnoreCase("l")
    val inputFolder = args(1)
    val outputFolder = args(2)
    val numberOfPartitions = args(3).toInt

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

    val inputDataRdd = sc.textFile(inputFolder, numberOfPartitions)

    val rowRdd = inputDataRdd.map(l => {
      val simpleDateFormat = new SimpleDateFormat("ddMMyyyy")
      val arrayList = l.split(",")

      if (arrayList.length > 16) {
        try {

          Row.fromSeq(Seq(arrayList(0),
            arrayList(0).split('/').length,
            arrayList(1),
            arrayList(2),
            arrayList(3).toInt,
            arrayList(4).toInt,
            arrayList(5).toInt,
            arrayList(6).toInt,
            arrayList(7).toLong,
            arrayList(8).toLong,
            new Timestamp(simpleDateFormat.parse(arrayList(16)).getTime)))
        } catch {
          case e:Exception => {
            println(l)
            null
          }

        }
      } else {
        null
      }
    }).filter(r => r != null)

    sql.sql("drop table if exists folder_stat_tmp")

    sql.sql("create external table  IF NOT EXISTS folder_stat_tmp ( " +
      " folder_nm string, " +
      " path_length int," +
      " owner string," +
      " group string," +
      " hive_file_cnt int," +
      " non_hive_file_cnt int," +
      " hive_folder_cnt int," +
      " non_hive_folder_cnt int," +
      " hive_data_size bigint," +
      " non_hive_data_size bigint," +
      " last_touched timestamp) " +
      " location '" + outputFolder + "/hive/folder_stat_tmp'")

    val emptyDf = sql.sql("select * from folder_stat_tmp limit 0")

    sql.createDataFrame(rowRdd, emptyDf.schema).registerTempTable("folder_stat")

    val topTenPaths = sql.sql("select owner, folder_nm, path_length, non_hive_file_cnt " +
      " from folder_stat " +
      " order by non_hive_file_cnt desc " +
      " limit 10")

    println("----")
    topTenPaths.collect().foreach(println)
    println("----")
    sc.stop()
  }
}
