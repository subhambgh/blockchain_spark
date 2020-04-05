package com.blockchain.test

import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.graphframes.GraphFrame

object BlockChainLocal {
  case class EdgeSchema(src: Int, dst: Int, txID: Int)
  case class AddressSchema(id: Int)

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("BlockChain3").setMaster("local[*]")
    val sparkContext = new SparkContext(conf).setCheckpointDir("/tmp")

    val spark = SparkSession
      .builder()
      .appName("BlockChain3")
      .config("spark.master", "local")
      .getOrCreate()

    val edgeSchema = Encoders.product[EdgeSchema].schema
    val edgeDf = spark.read.format("csv")
      .option("header", "false")
      .option("delimiter", "\t")
      .schema(edgeSchema)
      .load("E:/hw2/local/add_edges_s.txt")

    val addSchema = Encoders.product[AddressSchema].schema
    val addDf = spark.read.format("csv")
      .option("header", "false")
      .option("delimiter", "\t")
      .schema(addSchema)
      .load("E:/hw2/local/addresses.txt")

    val edgeFinalDf = edgeDf.filter(!(col("src")===lit(-1)))

    edgeFinalDf.show()

    val graph = GraphFrame(addDf, edgeFinalDf)

    val cc = graph.connectedComponents
      .run()
      .write
      .format("csv")
      .option("header","false")
      .option("delimiter", "\t")
      .save("E:/hw2/local/addr_jcsc.txt")

  }

}
