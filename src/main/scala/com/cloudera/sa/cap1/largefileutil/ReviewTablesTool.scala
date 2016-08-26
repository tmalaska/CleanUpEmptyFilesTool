package com.cloudera.sa.cap1.largefileutil

import java.io.File
import java.util.concurrent.{ExecutorService, Executors}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, RemoteIterator}

import scala.io.Source

object ReviewTablesTool {
  val summaryFolderStat = new FolderStats()
  var recommendedFileSizeInMb:Int = 0
  var threadsRunning = 0

  def main(args:Array[String]): Unit = {

    if (args.length == 0) {
      println("<inputFileForListOfTables> <FileSizeThresholdInMB> <recommendedFileSize> <threads>")
      return
    }

    val folderPath = args(0)
    val smallFileSizeThresholdInMb = args(1).toInt
    recommendedFileSizeInMb = args(2).toInt
    val numberOfThreads = args(3).toInt

    val tableFolders = Source.fromFile(new File(folderPath)).getLines().map(line => {
      line.split('\t')(4)
    })

    val fs = FileSystem.get(new Configuration())

    val pool: ExecutorService = Executors.newFixedThreadPool(numberOfThreads)

    try {
      tableFolders.foreach(folder => {

        pool.execute(new ProcessFolder(folder, fs,
          smallFileSizeThresholdInMb,
          recommendedFileSizeInMb))
      })

    } finally {
      while (threadsRunning != 0) {
        Thread.sleep(100)
      }
      pool.shutdown()
    }

    val possibleCompactionFileCount = (summaryFolderStat.numberOfFiles - summaryFolderStat.numberOfSmallFiles) +
      (1 + (summaryFolderStat.smallFileTotalSize / (recommendedFileSizeInMb * 1000000)))

    printFolderStats("Summary"  +
      "," + summaryFolderStat.numberOfFiles +
      "," + summaryFolderStat.numberOfSmallFiles +
      "," + summaryFolderStat.smallFileTotalSize +
      "," + possibleCompactionFileCount +
      "," + possibleCompactionFileCount.toDouble / summaryFolderStat.numberOfFiles.toDouble)

  }

  def printFolderStats(str:String): Unit = {
    this.synchronized {

      val possibleCompactionFileCount = (summaryFolderStat.numberOfFiles - summaryFolderStat.numberOfSmallFiles) +
        (1 + (summaryFolderStat.smallFileTotalSize / (recommendedFileSizeInMb * 1000000)))

      println(str + ",Summary"  +
        "," + summaryFolderStat.numberOfFiles +
        "," + summaryFolderStat.numberOfSmallFiles +
        "," + summaryFolderStat.smallFileTotalSize +
        "," + possibleCompactionFileCount +
        "," + possibleCompactionFileCount.toDouble / summaryFolderStat.numberOfFiles.toDouble +
        "," + threadsRunning)
    }
  }

  def addThread(): Unit = {
    this.synchronized {
      threadsRunning += 1
    }
  }

  def finishedThread(): Unit = {
    this.synchronized {
      threadsRunning -= 1
    }
  }

  def addToTotalStats(folderStats: FolderStats): Unit = {
    this.synchronized {
      summaryFolderStat += folderStats
    }
  }

  def collectFolderStats(fs:FileSystem,
                         folder:Path,
                         fileStatusIterator: RemoteIterator[FileStatus],
                         smallFileSizeThresholdInMb:Int ): FolderStats = {

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

class ProcessFolder(folder:String, fs:FileSystem,
                    smallFileSizeThresholdInMb:Int,
                    recommendedFileSizeInMb:Int) extends Runnable {
  def message = (Thread.currentThread.getName() + "\n").getBytes

  def run() {


    ReviewTablesTool.addThread()

    try {
      val path = new Path(folder)

      val fileStatusIterator = fs.listStatusIterator(path)

      val folderStats = ReviewTablesTool.collectFolderStats(fs, path, fileStatusIterator, smallFileSizeThresholdInMb)

      val possibleCompactionFileCount = (folderStats.numberOfFiles - folderStats.numberOfSmallFiles) +
        (1 + (folderStats.smallFileTotalSize / (recommendedFileSizeInMb * 1000000)))

      ReviewTablesTool.printFolderStats(folder +
        "," + folderStats.numberOfFiles +
        "," + folderStats.numberOfSmallFiles +
        "," + folderStats.smallFileTotalSize +
        "," + possibleCompactionFileCount +
        "," + possibleCompactionFileCount.toDouble / folderStats.numberOfFiles.toDouble)

      ReviewTablesTool.addToTotalStats(folderStats)
    } catch {
      case e: Exception => {
        ReviewTablesTool.printFolderStats(folder + "," + "0" +
          "," + "0" +
          "," + "0" +
          "," + "0" +
          "," + "0")
      }
    } finally {
      ReviewTablesTool.finishedThread()
    }
  }
}


