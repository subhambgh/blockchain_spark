package com.blockchain.app

import com.blockchain.helper.ReadPropFromS3
import org.apache.spark.sql.{Encoders, SparkSession}
import org.graphframes._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._

object BlockChain3 {

  case class EdgeSchema(src: Int, dst: Int, txID: Int)
  case class AddressSchema(id: Int)

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("BlockChain3")
    val sparkContext = new SparkContext(conf).setCheckpointDir("/tmp")

    val spark = SparkSession
      .builder()
      .appName("BlockChain3")
      .getOrCreate()

    val edgeSchema = Encoders.product[EdgeSchema].schema
    val edgeDf = spark.read.format("csv")
      .option("header", "false")
      .option("delimiter", "\t")
      .schema(edgeSchema)
      .load(ReadPropFromS3.getProperties("edge"))

    val addSchema = Encoders.product[AddressSchema].schema
    val addDf = spark.read.format("csv")
      .option("header", "false")
      .option("delimiter", "\t")
      .schema(addSchema)
      .load(ReadPropFromS3.getProperties("add"))

    val edgeFinalDf = edgeDf.filter(!(col("src")===lit(-1)))

    val graph = GraphFrame(addDf, edgeFinalDf)

    val cc = graph.connectedComponents
      .run()
      .write
      .format("csv")
      .option("header","false")
      .option("delimiter", "\t")
      .save(ReadPropFromS3.getProperties("fs.default.name")+"blockchain-bucket/data/addr_jcsc.txt")

  }

}