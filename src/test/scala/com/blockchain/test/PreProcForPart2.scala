package com.blockchain.test

import com.blockchain.helper.ReadPropFromS3
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Encoders, SparkSession}
import org.graphframes._

object PreProcForPart2 {

  case class EdgeSchema(src: Int, dst: Int, txID: Int)
  case class AddressSchema(id: Int)

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("BlockChain3").setMaster("local")
    val sparkContext = new SparkContext(conf).setCheckpointDir("/tmp")

    val spark = SparkSession
      .builder()
      .appName("PreProcForPart2")
      .config("spark.master", "local")
      .getOrCreate()

    val edgeSchema = Encoders.product[EdgeSchema].schema
    val edgeDf = spark.read.format("csv")
      .option("header", "false")
      .option("delimiter", "\t")
      .schema(edgeSchema)
      .load("E:/hw2/lan/addr_edges.txt")
      .distinct()

    val addSchema = Encoders.product[AddressSchema].schema
    val addDf = spark.read.format("csv")
      .option("header", "false")
      .option("delimiter", "\t")
      .schema(addSchema)
      .load("E:/hw2/lan/addresses.txt")

    val graph = GraphFrame(addDf, edgeDf)

    /**
     *
     * most interesting part -PART II
     *          - using graphx
     *  Algorithm
     *    Option 1.
     *      - Graph
     *      - create vertices - comprising of the addrID's from the addresses.dat
     *      - create edges - using data from the preprocessing step
     *      - now use connected component analysis - to generate User Id's
     *    Option 2. using partition over transaction id and use rowcount to give a user Id
     *
     * finally, Write to S3
     * */
    val cc = graph.connectedComponents
      .run()
      .write
      .format("csv")
      .option("header","false")
      .option("delimiter", "\t")
      .save("E:/hw2/lan/addr_jcsc.txt")
  }

}