package com.blockchain.app

import com.blockchain.helper.{ReadFromHDFS, ReadPropFromS3, WriteToS3}
import org.apache.spark.sql.functions.{col, max}
import org.apache.spark.sql.{Encoders, SparkSession}

object BlockChain2 {

  case class TxnSchema(txID: Int, blockID: Int, n_inputs: Int, n_outputs: Int)
  case class TxnHashSchema(txID: Int, hash: String)

  def main(args: Array[String]): Unit = {

    val sumTotalOpTrans:BigDecimal = BigDecimal(args(0))
    val numAddress:BigDecimal = BigDecimal(ReadFromHDFS.readNumLineFromHDFS(ReadPropFromS3.getProperties("readLinesAdd")))
    val numTrans: BigDecimal  = BigDecimal(ReadFromHDFS.readNumLineFromHDFS(ReadPropFromS3.getProperties("readLinesTx")))
    val numBlocks: BigDecimal  = BigDecimal(ReadFromHDFS.readNumLineFromHDFS(ReadPropFromS3.getProperties("readLinesBh")))

    val spark = SparkSession
      .builder()
      .appName("BlockChain2")
      .getOrCreate()

    var txnSchema = Encoders.product[TxnSchema].schema
    var txnDf = spark.read.format("csv").
      option("header", "false").
      option("delimiter", "\t").
      schema(txnSchema).
      load(ReadPropFromS3.getProperties("tx")).
      cache()

    var txnHashSchema = Encoders.product[TxnHashSchema].schema
    var txnHashDf = spark.read.format("csv").
      option("header", "false").
      option("delimiter", "\t").
      schema(txnHashSchema).
      load(ReadPropFromS3.getProperties("txh"))

    var numTransWithZeroIp = txnDf.select(col("txID")).where(col("n_inputs") === 0).count()

    txnDf = txnDf.select(txnDf.col("*")).where(col("n_inputs")=== txnDf.select(max("n_inputs")).first().get(0))

    var finalTxDf = txnDf.as("txnDf").join(txnHashDf.as("txnHashDf"), txnDf("txID") === txnHashDf("txID"), "inner").
      select(col("txnDf.*"), col("txnHashDf.hash"))
    var maxIp = finalTxDf.collectAsList().toString

    /*
        5. What is the transaction that has the greatest number of inputs? How
        many inputs exactly? Show the hash of that transaction. If there are
        multiple transactions that have the same greatest number of inputs, show
        all of them.  */

    /* 6. What is the average transaction value? Transaction value is the sum of
    all outputs’ value.
     /*7. How many coinbase transactions are there in the dataset? */
    8. What is the average number of transactions per block?
     */

    WriteToS3.write("1. number of transactions: " + numTrans + " & addresses:" + numAddress)
    WriteToS3.write("4.2. total number of transactions/address : " + numTrans / numAddress)
    WriteToS3.write("5." + "\n" + maxIp)
    WriteToS3.write("6. average transaction value :" + sumTotalOpTrans / numTrans)
    WriteToS3.write("7. Number of coinbase transactions :" + numTransWithZeroIp)
    WriteToS3.write("8. average number of transactions per block :" + numBlocks / numTrans)
    WriteToS3.close()
  }

}

