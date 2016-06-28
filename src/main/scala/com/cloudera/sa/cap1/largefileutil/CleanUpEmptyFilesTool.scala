package com.cloudera.sa.cap1.largefileutil

import java.util.Date

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, RemoteIterator, Path, FileSystem}

object CleanUpEmptyFilesTool {
  def main(args:Array[String]): Unit = {
    if (args.length == 0) {
      println("--- Help:")
      println(" LargeFileUtil <cmd> <path>")
      println("--- Cmd List")
      println(" ls -> this will list out files in the folder and sub-folders")
      println(" ls-zero -> this will list out all zero size files in the folder and sub-folders")
      println(" rm-zero -> this will remove all zero size files in the folder and sub-folders")
      return
    }

    val fs = FileSystem.get(new Configuration())

    val cmd = args(0)
    val path = args(1)

    val fileStatusIterator = fs.listStatusIterator(new Path(path))

    if (cmd.equalsIgnoreCase("ls")) {
      processLsOnFolder(fs, new Path(path), fileStatusIterator)
    } else if (cmd.equalsIgnoreCase("ls-zero")) {
      processLsZeroOnFolder(fs, new Path(path), fileStatusIterator)
    } else if (cmd.equalsIgnoreCase("rm-zero")) {
      processRmZeroOnFolder(fs, new Path(path), fileStatusIterator)
    }
  }

  def processLsZeroOnFolder(fs:FileSystem, folder:Path, fileStatusIterator: RemoteIterator[FileStatus]): Unit = {
    var fileCounter = 0
    while (fileStatusIterator.hasNext) {
      val fileStatus = fileStatusIterator.next()
      val filePath = fileStatus.getPath
      val fileOwner = fileStatus.getOwner
      val modTime = fileStatus.getModificationTime
      val len = fileStatus.getLen
      val replication = fileStatus.getReplication
      val group = fileStatus.getGroup
      val permission = fileStatus.getPermission

      if (fileStatus.isFile && len == 0 && !filePath.getName.startsWith("_") && !filePath.getName.startsWith(".")) {
        println(permission + " " + replication + " " + fileOwner + " " + group + " " + len + " " + new Date(modTime) + " " + filePath)
        fileCounter += 1
      }

      if (fileStatus.isDirectory) {
        processLsZeroOnFolder(fs, fileStatus.getPath, fs.listStatusIterator(fileStatus.getPath))
      }
    }
    println(">> finished: " + " " + folder + " : fileCount:" + fileCounter)
  }

  def processRmZeroOnFolder(fs:FileSystem, folder:Path, fileStatusIterator: RemoteIterator[FileStatus]): Unit = {
    var fileCounter = 0
    while (fileStatusIterator.hasNext) {
      val fileStatus = fileStatusIterator.next()
      val filePath = fileStatus.getPath
      val fileOwner = fileStatus.getOwner
      val modTime = fileStatus.getModificationTime
      val len = fileStatus.getLen
      val replication = fileStatus.getReplication
      val group = fileStatus.getGroup
      val permission = fileStatus.getPermission

      if (fileStatus.isFile && len == 0 && !filePath.getName.startsWith("_") && !filePath.getName.startsWith(".")) {
        println(permission + " " + replication + " " + fileOwner + " " + group + " " + len + " " + new Date(modTime) + " " + filePath)
        fileCounter += 1
        fs.delete(fileStatus.getPath, false)
      }

      if (fileStatus.isDirectory) {
        processRmZeroOnFolder(fs, fileStatus.getPath, fs.listStatusIterator(fileStatus.getPath))
      }
    }
    println(">> finished: " + " " + folder + " : fileCount:" + fileCounter)
  }

  def processLsOnFolder(fs:FileSystem, folder:Path, fileStatusIterator: RemoteIterator[FileStatus]): Unit = {
    var fileCounter = 0
    while (fileStatusIterator.hasNext) {
      val fileStatus = fileStatusIterator.next()
      val filePath = fileStatus.getPath
      val fileOwner = fileStatus.getOwner
      val modTime = fileStatus.getModificationTime
      val len = fileStatus.getLen
      val replication = fileStatus.getReplication
      val group = fileStatus.getGroup
      val permission = fileStatus.getPermission

      println(permission + " " + replication + " " + fileOwner + " " + group + " " + len + " " + new Date(modTime) + " " + filePath)

      fileCounter += 1
      if (fileStatus.isDirectory) {
        processLsOnFolder(fs, fileStatus.getPath, fs.listStatusIterator(fileStatus.getPath))
      }
    }
    println(">> finished: " + " " + folder + " : fileCount:" + fileCounter)
  }
}
