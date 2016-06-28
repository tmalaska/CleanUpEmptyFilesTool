package com.cloudera.sa.cap1.largefileutil

import java.io.{BufferedWriter, OutputStreamWriter}
import java.util.Random

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}


object GenTestFolder {
  def main(args:Array[String]): Unit = {
    val fs = FileSystem.get(new Configuration())

    val r = new Random
    val numOfFiles = args(0).toInt
    val path = args(1)
    var currentPath = path
    for (i <- 0 until numOfFiles) {
      val n = r.nextInt(100)
      if (n % 12 == 0) {
        currentPath = path
      }
      if (n < 30) {
        //Make empty file
        fs.create(new Path(currentPath + "/" + "zeroFile." + i + ".txt")).close()
        println("new zero file:" + new Path(currentPath + "/" + "zeroFile." + i + ".txt"))
      } else if (n < 80) {
        //Make data file
        val stream = fs.create(new Path(currentPath + "/" + "dataFile." + i + ".txt"))
        val writer = new BufferedWriter(new OutputStreamWriter(stream))
        writer.write("Foobar " + n + " " + i)
        writer.close()
        println("new full file:" + new Path(currentPath + "/" + "dataFile." + i + ".txt"))
      } else {
        //make new folder
        fs.mkdirs(new Path(currentPath + "/" + "folder." + i))
        currentPath = currentPath + "/" + "folder." + i
        println("new Folder:" + new Path(currentPath + "/" + "folder." + i))
      }
    }

  }
}
