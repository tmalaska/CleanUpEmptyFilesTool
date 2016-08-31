package com.cloudera.sa.cap1.d3helper

import java.io.{File, PrintWriter}
import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext

import scala.io.Source

object CirclePackingGen {
  def main(args: Array[String]): Unit = {

    if (args.length == 0) {
      println("Args: <runLocal> <inputFile> " +
        "<outputFile> " +
        "<tmpFolder> " +
        "<numberOfPartition> ")
      return
    }

    val runLocal = args(0).equalsIgnoreCase("l")
    val inputFile = args(1)
    val outputFile = args(2)
    val tmpFolder = args(3)
    val numberOfPartitions = args(4).toInt

    val sc: SparkContext = if (runLocal) {
      val sparkConfig = new SparkConf()
      sparkConfig.set("spark.broadcast.compress", "false")
      sparkConfig.set("spark.shuffle.compress", "false")
      sparkConfig.set("spark.shuffle.spill.compress", "false")
      new SparkContext("local[4]", "TableStatsSinglePathMain", sparkConfig)
    } else {
      val sparkConfig = new SparkConf().setAppName("TableStatsSinglePathMain")
      new SparkContext(sparkConfig)
    }

    val sql = new HiveContext(sc)

    val inputDataRdd = sc.textFile(inputFile, numberOfPartitions)

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
            //println(l)
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
      " location '" + tmpFolder + "/hive/folder_stat_tmp'")

    val emptyDf = sql.sql("select * from folder_stat_tmp limit 0")

    sql.createDataFrame(rowRdd, emptyDf.schema).registerTempTable("folder_stat")

    val level5:RDD[(String, Row)] = sql.sql("select owner, folder_nm, path_length, non_hive_file_cnt " +
      " from folder_stat " +
      " where path_length = 5 and " +
      " non_hive_file_cnt > 100").map(r => {(r(1).toString, r)})

    val level6:RDD[(String, Row)] = sql.sql("select owner, folder_nm, path_length, non_hive_file_cnt " +
      " from folder_stat " +
      " where path_length = 6 and non_hive_file_cnt > 100").map(r => {
      val folderName = r(1).toString
      val parentFolder = folderName.substring(0, folderName.lastIndexOf('/'))
      (parentFolder, r)
    })
    println("----")

    /*
    val level6:RDD[(String, Row)] = sql.sql("select owner, folder_nm, path_length, non_hive_file_cnt " +
      " from folder_stat " +
      " where path_length = 6 ").rdd.map(r => {
      val folderName = r(1).toString
      val parentFolder = folderName.substring(0, folderName.lastIndexOf('/'))
      (parentFolder, r)
    })
    */

    val pw = new PrintWriter(new File(outputFile ))

    val nl = sys.props("line.separator")


    pw.write("{" + nl)
    pw.write("  \"name\": \"flare\"," + nl)
    pw.write("  \"children\": [" + nl)

    var isFirst = true
    level5.cogroup(level6).map(r => {
      val folderName = r._1.toString
      val parentFolder = folderName.substring(0, folderName.lastIndexOf('/'))
      (parentFolder, r)
    }).groupByKey().collect().foreach(r => {
      if(isFirst) {
        isFirst = false
      } else {
        pw.write("," + nl)
      }
      pw.write("    {" + nl)
      pw.write("      \"name\": \"" + r._1 + "\"," + nl)
      pw.write("      \"children\": [" + nl)

      var isFirstSub = true

      r._2.foreach(sr => {
        sr._1
        if(isFirstSub) {
          isFirstSub = false
        } else {
          pw.write("," + nl)
        }
        pw.write("        {" + nl)
        pw.write("          \"name\": \"" + r._1 + "\"," + nl)
        pw.write("          \"children\": [" + nl)

        var isFirstSubSub = true
        sr._2._2.foreach(ssr => {
          val nonHiveFiles = ssr(3).asInstanceOf[Int]
          if (nonHiveFiles > 100) {
            if (isFirstSubSub) {
              isFirstSubSub = false
            } else {
              pw.write("," + nl)
            }
            var folderNm = ssr(1).toString
            folderNm = folderNm.substring(folderNm.lastIndexOf('/') + 1)
            val nonHiveFiles = ssr(3)
            pw.write("             {\"name\": \"" + folderNm + "\", \"size\": " + nonHiveFiles + "}")
          }
        })

        if (isFirstSubSub) {
          pw.write("             {\"name\": \"empty\", \"size\": " + 1 + "}")
        }
        pw.write(nl)

        pw.write("          ]" + nl)
        pw.write("        }")
      })

      if (isFirstSub) {
        pw.write("           {\"name\": \"empty\", \"size\": " + 1 + "}")
      }
      pw.write(nl)

      pw.write("        ]" + nl)
      pw.write("      }")
    })
    pw.write(nl)
    pw.write("    ]" + nl)
    pw.write("  }" + nl)

    pw.close

    /*
    pw.write("{" + nl)
    pw.write("  \"name\": \"flare\"," + nl)
    pw.write("  \"children\": [" + nl)


    var isFirst = true
    level5.cogroup(level6).collect().foreach(r => {

      if(isFirst) {
        isFirst = false
      } else {
        pw.write("," + nl)
      }
      pw.write("    {" + nl)
      pw.write("      \"name\": \"" + r._1 + "\"," + nl)
      pw.write("      \"children\": [" + nl)

      var isFirstSub = true

      r._2._2.foreach(sr => {
        val nonHiveFiles = sr(3).asInstanceOf[Int]
        if (nonHiveFiles > 100) {
          if (isFirstSub) {
            isFirstSub = false
          } else {
            pw.write("," + nl)
          }
          var folderNm = sr(1).toString
          folderNm = folderNm.substring(folderNm.lastIndexOf('/') + 1)
          val nonHiveFiles = sr(3)
          pw.write("        {\"name\": \"" + folderNm + "\", \"size\": " + nonHiveFiles + "}")
        }
      })

      if (isFirstSub) {
        pw.write("        {\"name\": \"empty\", \"size\": " + 1 + "}")
      }
      pw.write(nl)

      pw.write("        ]" + nl)
      pw.write("      }")
    })
    pw.write(nl)
    pw.write("    ]" + nl)
    pw.write("  }" + nl)

    pw.close
*/



    sc.stop()
  }
}
