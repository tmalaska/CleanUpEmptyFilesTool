package com.cloudera.sa.cap1.largefileutil

import java.io.File
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Callable, ExecutorService, Executors, Future}

import com.cloudera.sa.cap1.largefileutil.WhereAreTheFiles.FolderWhereStats
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, RemoteIterator}

import scala.collection.mutable
import scala.io.Source

object WhereAreTheFiles {

  var tableFolderSet: Set[String] = null
  var globalFolderStats = new FolderWhereStats
  var numberOfThreads:Int = 0
  var pool: ExecutorService= null
  val threadsRunningAtomic:AtomicInteger = new AtomicInteger(0)

  def main(args:Array[String]): Unit = {
    if (args.length == 0) {
      println("<inputFileForListOfTables> <startingPath> <numOfThreads>")
      return
    }

    val folderPath = args(0)
    val startingPath = new Path(args(1))
    numberOfThreads = args(2).toInt

    pool = Executors.newFixedThreadPool(numberOfThreads * 2)

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

    val folderString = folder.toString

    var isTable = false

    tableFolderSet.foreach(p => {
      if (folderString.startsWith(p)) {
        isTable = true
      }
    })

    folderWhereStat.addDirector(isTable)
    globalFolderStats.addDirector(isTable)

    val futureList = new mutable.MutableList[Future[FolderWhereStats]]

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

        var runInThread = true

        if (threadsRunningAtomic.incrementAndGet() < numberOfThreads) {
          runInThread = false
          futureList += pool.submit(new CollectFolderStatsCallable(fs, fileStatus))

        } else {
          threadsRunningAtomic.decrementAndGet()

          for (future <- futureList) {
            folderWhereStat += future.get()
            threadsRunningAtomic.decrementAndGet()
          }
          futureList.clear()
        }

        if (runInThread) {
          val childrenStats = collectFolderStats(fs,
            fileStatus.getPath,
            fs.listStatusIterator(fileStatus.getPath))

          childrenStats
        }

      } else {
        folderWhereStat.addNormalFile(isTable, len, modTime)
        globalFolderStats.addNormalFile(isTable, len, modTime)
      }
    }

    for (future <- futureList) {
      folderWhereStat += future.get()
      threadsRunningAtomic.decrementAndGet()
    }
    futureList.clear()


    val folderStatus = fs.getFileStatus(folder)

    if (!folderString.contains("=")) {
      println(folderString + "," + folderStatus.getOwner + "," + folderStatus.getGroup + "," + folderWhereStat.toString() + "," + globalFolderStats)
    }

    folderWhereStat
  }



  class FolderWhereStats(var tableFileCount:Int = 0,
                         var nonTableFileCount:Int = 0,
                         var tableFolderCount:Int = 0,
                         var nonTableFolderCount:Int = 0,
                         var tableFileSize:Long = 0,
                         var nonTableFileSize:Long = 0,
                         var earliestChangeDate:Long = Long.MaxValue) {

    def addNormalFile (isTableFile:Boolean, len:Long, modTime:Long): Unit = {
      if (isTableFile) {
        tableFileCount += 1
        tableFileSize += len
        earliestChangeDate = Math.min(modTime, earliestChangeDate)
      } else {
        nonTableFileCount += 1
        nonTableFileSize += len
        earliestChangeDate = Math.min(modTime, earliestChangeDate)
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
      earliestChangeDate = Math.min(this.earliestChangeDate, earliestChangeDate)
    }

    override def toString(): String = {
      tableFileCount + "," +
        nonTableFileCount + "," +
        tableFolderCount + "," +
        nonTableFolderCount + "," +
        tableFileSize + "," +
        nonTableFileSize + "," +
        earliestChangeDate
    }
  }
}

class CollectFolderStatsCallable(val fs:FileSystem,
                                 val fileStatus: FileStatus) extends Callable[FolderWhereStats] {
  var internalStat = 0

  override def call(): FolderWhereStats = {
    internalStat = 1
    val result = WhereAreTheFiles.collectFolderStats(fs,
      fileStatus.getPath,
      fs.listStatusIterator(fileStatus.getPath))
    internalStat = 2
    result
  }
}
