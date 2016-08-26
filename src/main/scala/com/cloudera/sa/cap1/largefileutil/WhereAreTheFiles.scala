package com.cloudera.sa.cap1.largefileutil

import java.io.File
import java.util.concurrent.{ExecutorService, Executors}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, RemoteIterator}

import scala.io.Source

object WhereAreTheFiles {

  var tableFolderSet: Set[String] = null

  def main(args:Array[String]): Unit = {
    if (args.length == 0) {
      println("<inputFileForListOfTables> <startingPath>")
      return
    }

    val folderPath = args(0)
    val startingPath = new Path(args(1))

    tableFolderSet = Source.fromFile(new File(folderPath)).getLines().map(line => {
      new Path(line.split('\t')(4)).toString
    }).toSet

    val fs = FileSystem.get(new Configuration())

    println("summary," + collectFolderStats(fs, startingPath, fs.listStatusIterator(startingPath)).toString())
  }

  def collectFolderStats(fs:FileSystem,
                         folder:Path,
                         fileStatusIterator: RemoteIterator[FileStatus] ): FolderWhereStats = {

    val folderWhereStat = new FolderWhereStats

    var folderString = folder.toString
    val colonIndex = folderString.indexOf(':')
    if (colonIndex > 0) {
      folderString = folderString.substring(colonIndex + 1)
    }

    var isTable = false

    tableFolderSet.foreach(p => {
      if (folderString.startsWith(p)) {
        isTable = true
      }
    })

    folderWhereStat.addDirector(isTable)

    while (fileStatusIterator.hasNext) {
      val fileStatus = fileStatusIterator.next()
      val filePath = fileStatus.getPath
      val fileOwner = fileStatus.getOwner
      val modTime = fileStatus.getModificationTime
      val len = fileStatus.getLen
      val replication = fileStatus.getReplication
      val group = fileStatus.getGroup
      val permission = fileStatus.getPermission



      if (fileStatus.isDirectory) {

        folderWhereStat += collectFolderStats(fs,
          fileStatus.getPath,
          fs.listStatusIterator(fileStatus.getPath))
      } else {
        folderWhereStat.addNormalFile(isTable)
      }
    }
    println(folderString + "," + folderWhereStat.toString())

    folderWhereStat
  }

  class FolderWhereStats(var tableFileCount:Int = 0,
                    var nonTableFileCount:Int = 0,
                         var tableFolderCount:Int = 0,
                         var nonTableFolderCount:Int = 0) {

    def addNormalFile (isTableFile:Boolean): Unit = {
      if (isTableFile) {
        tableFileCount += 1
      } else {
        nonTableFileCount += 1
      }
    }

    def addDirector(isTableFile:Boolean): Unit = {
      if (isTableFile) {
        tableFolderCount += 1
      } else {
        nonTableFolderCount += 1
      }
    }

    def += (that:FolderWhereStats): Unit = {
      tableFileCount += that.tableFileCount
      nonTableFileCount += that.nonTableFileCount
      tableFolderCount += that.tableFolderCount
      nonTableFolderCount += that.nonTableFolderCount
    }

    override def toString(): String = {
      tableFileCount + "," +
        nonTableFileCount + "," +
        tableFolderCount + "," +
        nonTableFolderCount
    }

  }

}
