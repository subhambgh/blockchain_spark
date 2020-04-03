package com.blockchain.app

import java.io.File

import org.apache.spark.sql.functions.{col, max}
import org.apache.spark.sql.{Encoders, SparkSession}

object BlockChainLocal {
  case class TxnAddSchema(srcAdd: Int, destAdd: Int, txId: Int)
  case class TxnOutSchema(txID: Int, destAdd: Int)

  def main(args: Array[String]): Unit = {

      val sumTotalOpTrans:BigDecimal = 1
      val numAddress:BigDecimal = 1

      val spark = SparkSession
        .builder()
        .appName("BlockChain2")
        .config("spark.master", "local")
        .getOrCreate()

      /*
      5. What is the transaction that has the greatest number of inputs? How
      many inputs exactly? Show the hash of that transaction. If there are
      multiple transactions that have the same greatest number of inputs, show
      all of them.  */

      var txnSchema = Encoders.product[TxnAddSchema].schema
      var txnDf = spark.read.format("csv").
        option("header", "false").
        option("delimiter", "\t").
        schema(txnSchema).
        load("E:/hw2/local/add_edges_s.txt")

      var txnHashSchema = Encoders.product[TxnOutSchema].schema
      var txnHashDf = spark.read.format("csv").
        option("header", "false").
        option("delimiter", "\t").
        schema(txnHashSchema).
        load("E:/hw2/local/tx_out.txt")

}

}

