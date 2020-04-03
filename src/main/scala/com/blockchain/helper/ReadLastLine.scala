package com.blockchain.helper

import java.io.File
import java.nio.charset.Charset

import org.apache.commons.io.input.ReversedLinesFileReader

object ReadLastLine {
  def readLastLine(file:String):String = {
    val fileReader = new ReversedLinesFileReader(new File(file),Charset.defaultCharset())
    return fileReader.readLine().split("\t")(0).toString
  }
}
