package com.blockchain.helper

import java.io.{BufferedReader, File, FileInputStream, InputStreamReader}
import java.math.BigInteger
import java.net.URI
import java.nio
import java.nio.charset.Charset
import java.nio.file

import org.apache.commons.io.input.ReversedLinesFileReader
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object ReadFromHDFS {
  val fs = FileSystem.get(new URI(ReadPropFromS3.getProperties("fs.default.name")), new Configuration())

  def readNumLineFromHDFS(file: String): Long = {
    var newlineCount = 0L
    val br = new BufferedReader(new InputStreamReader(fs.open(new Path(file))))
    Stream.continually(br.readLine()).takeWhile(_ != null).foreach(line =>newlineCount = newlineCount+1)
    newlineCount
  }

  def readLastLineFromHDFS(file: String): String = {
    var lastLine:String = new String();
    val br = new BufferedReader(new InputStreamReader(fs.open(new Path(file))))
    Stream.continually(br.readLine()).takeWhile(_ != null).foreach(line => lastLine = line)
    lastLine.split("\t")(0)
  }

}
