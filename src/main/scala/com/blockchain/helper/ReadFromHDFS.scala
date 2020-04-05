package com.blockchain.helper

import java.io.{BufferedReader, File, FileInputStream, InputStreamReader}
import java.math.BigInteger
import java.net.URI
import java.nio.charset.Charset

import org.apache.commons.io.input.ReversedLinesFileReader
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object ReadFromHDFS {
  val fs = FileSystem.get(new URI(ReadPropFromS3.getProperties("fs.default.name")), new Configuration())

  def readLastLineFromHDFS(file: String): String = {
    val fileReader = new ReversedLinesFileReader(new File(file), Charset.defaultCharset())
    return fileReader.readLine().split("\t")(0)
  }
  def readNumLineFromHDFS(file: String): BigInt = {
    var newlineCount = BigInt("0")
    val br = new BufferedReader(new InputStreamReader(fs.open(new Path(file))))
    br.readLine().takeWhile(_ != null).foreach(line =>newlineCount = newlineCount+1)
    newlineCount
  }

}
