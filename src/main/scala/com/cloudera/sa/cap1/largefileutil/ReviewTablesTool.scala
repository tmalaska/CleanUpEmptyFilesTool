package com.cloudera.sa.cap1.largefileutil

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, RemoteIterator}

import scala.io.Source

object ReviewTablesTool {
  def main(args:Array[String]): Unit = {

    if (args.length == 0) {
      println("<inputFileForListOfTables> <FileSizeThresholdInMB> <recommendedFileSize>")
    }

    val folderPath = args(0)
    val smallFileSizeThresholdInMb = args(1).toInt
    val recommendedFileSizeInMb = args(2).toInt

    val tableFolders = Source.fromFile(new File(folderPath)).getLines().map(line => {
      println(line)
      println(line.split('\t').length)
      line.split('\t')(4)
    })

    val fs = FileSystem.get(new Configuration())

    val folderStatMap = tableFolders.map(folder => {

      val path = new Path(folder)

      val fileStatusIterator = fs.listStatusIterator(path)

      var totalFiles = 0
      var totalSmallerFiles = 0

      val folderStats = collectFolderStats(fs, path, fileStatusIterator, smallFileSizeThresholdInMb)

      (path, folderStats)
    })

    val summaryFolderStat = new FolderStats()

    folderStatMap.foreach(r => {

      println("Starting: " + r._1)
      val possibleCompactionFileCount = (r._2.numberOfFiles - r._2.numberOfSmallFiles) +
        (1 + (r._2.smallFileTotalSize / (recommendedFileSizeInMb * 1000000)))

      println("Path:" + r._1 +
        ",NumberOfFiles:" + r._2.numberOfFiles +
        ",NumberOfSmallFiles:" + r._2.numberOfSmallFiles +
        ",SizeOfSmallFiles:" + r._2.smallFileTotalSize +
        ",possibleCompactionFileCount:" + possibleCompactionFileCount +
        ",reductionOfFilePercentage:" + possibleCompactionFileCount.toDouble / r._2.numberOfFiles.toDouble)

      summaryFolderStat += r._2
    })

    val possibleCompactionFileCount = (summaryFolderStat.numberOfFiles - summaryFolderStat.numberOfSmallFiles) +
      (1 + (summaryFolderStat.smallFileTotalSize / (recommendedFileSizeInMb * 1000000)))

    println("Path:Summary"  +
      ",NumberOfFiles:" + summaryFolderStat.numberOfFiles +
      ",NumberOfSmallFiles:" + summaryFolderStat.numberOfSmallFiles +
      ",SizeOfSmallFiles:" + summaryFolderStat.smallFileTotalSize +
      ",possibleCompactionFileCount:" + possibleCompactionFileCount +
      ",reductionOfFilePercentage:" + possibleCompactionFileCount.toDouble / summaryFolderStat.numberOfFiles.toDouble)

  }

  def collectFolderStats(fs:FileSystem,
                         folder:Path,
                         fileStatusIterator: RemoteIterator[FileStatus],
                         smallFileSizeThresholdInMb:Int ): FolderStats = {
    print(".")
    val folderStats = new FolderStats
    while (fileStatusIterator.hasNext) {
      val fileStatus = fileStatusIterator.next()
      val filePath = fileStatus.getPath
      val fileOwner = fileStatus.getOwner
      val modTime = fileStatus.getModificationTime
      val len = fileStatus.getLen
      val replication = fileStatus.getReplication
      val group = fileStatus.getGroup
      val permission = fileStatus.getPermission

      if (fileStatus.isFile && len < smallFileSizeThresholdInMb * 1000 * 1000) {
        folderStats.addSmallFile(len)
      } else {
        folderStats.addNormalFile()
      }

      if (fileStatus.isDirectory) {
        folderStats += collectFolderStats(fs,
          fileStatus.getPath,
          fs.listStatusIterator(fileStatus.getPath),
          smallFileSizeThresholdInMb)
      }
    }
    folderStats
  }
}

class FolderStats(var numberOfFiles:Int = 0,
                  var numberOfSmallFiles:Int = 0,
                  var smallFileTotalSize:Long = 0) {

  def addSmallFile (fileSize:Long): Unit = {
    numberOfFiles += 1
    numberOfSmallFiles += 1
    smallFileTotalSize += fileSize
  }

  def addNormalFile (): Unit = {
    numberOfFiles += 1
  }

  def += (that:FolderStats): Unit = {
    numberOfFiles += that.numberOfFiles
    numberOfSmallFiles += that.numberOfSmallFiles
    smallFileTotalSize += that.smallFileTotalSize
  }

}


