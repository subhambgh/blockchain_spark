package com.blockchain.helper

import java.io.{BufferedWriter, File, FileOutputStream, OutputStreamWriter}

import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}

object Writer {
  val s3Client: AmazonS3 = AmazonS3ClientBuilder.standard.withRegion(Regions.US_EAST_1).build
  var file:File = new File("op.txt")
  val writer:BufferedWriter = new BufferedWriter(new OutputStreamWriter(
    new FileOutputStream(file, true), "UTF-8"));

  def write(output: String) {
    writer.write(output)
    writer.newLine()
    println("####@@@@$$$:"+output +"\n")
  }

  def close(): Unit ={
    writer.close()
    s3Client.putObject("blockchain-emr-bucket", ReadProperties.getProperties("writeTo"), file)
  }
}
