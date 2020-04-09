package com.blockchain.app

import com.blockchain.helper.{ReadFromHDFS, ReadPropFromS3, WriteToS3}
import org.apache.spark.sql.functions.{col, max}
import org.apache.spark.sql.{Encoders, SparkSession}

object Part1_2 {

  case class TxnSchema(txID: Int, blockID: Int, n_inputs: Int, n_outputs: Int)
  case class TxnHashSchema(txID: Int, hash: String)

  def main(args: Array[String]): Unit = {

    val sumTotalOpTrans = args(0).toLong
    val numAddress = ReadFromHDFS.readNumLineFromHDFS(ReadPropFromS3.getProperties("readLinesAdd"))
    val numTrans  = ReadFromHDFS.readNumLineFromHDFS(ReadPropFromS3.getProperties("readLinesTx"))
    val numBlocks  = ReadFromHDFS.readNumLineFromHDFS(ReadPropFromS3.getProperties("readLinesBh"))

    val spark = SparkSession
      .builder()
      .appName("Part1_2")
      .getOrCreate()

    val txnSchema = Encoders.product[TxnSchema].schema
    var txnDf = spark.read.format("csv").
      option("header", "false").
      option("delimiter", "\t").
      schema(txnSchema).
      load(ReadPropFromS3.getProperties("tx")).
      cache()

    val txnHashSchema = Encoders.product[TxnHashSchema].schema
    val txnHashDf = spark.read.format("csv").
      option("header", "false").
      option("delimiter", "\t").
      schema(txnHashSchema).
      load(ReadPropFromS3.getProperties("txh"))

    /**
     * coinbase transactions are trans with 0 inputs
     * To calculate that we have a col(n_inputs) in the tx.dat data
     * we can utilize that to calculate the number of coinbase trans
     *
     * */
    val numTransWithZeroIp = txnDf.select(col("txID")).where(col("n_inputs") === 0).count()

    /**
     * again using col(n_inputs) we can calculate trans with max inputs
     * and also join with txh.dat to get the transaction hashes
     *
     * */
    txnDf = txnDf.select(txnDf.col("*"))
      .where(col("n_inputs")=== txnDf.select(max("n_inputs")).first().get(0))
    var finalTxDf = txnDf.as("txnDf")
      .join(txnHashDf.as("txnHashDf"), txnDf("txID") === txnHashDf("txID"), "inner").
      select(col("txnDf.*"), col("txnHashDf.hash"))
    var maxIp = finalTxDf.collectAsList().toString

    /**
     * write files to s3
     *
     * */
    WriteToS3.write("numBlocks="+numBlocks)
    WriteToS3.write("1. number of transactions: " + numTrans + " & addresses:" + numAddress)
    WriteToS3.write("4.2. total number of transactions/address : " + numTrans.toDouble / numAddress)
    WriteToS3.write("5." + "\n" + maxIp)
    WriteToS3.write("6. average transaction value :" + sumTotalOpTrans.toDouble / numTrans)
    WriteToS3.write("7. Number of coinbase transactions :" + numTransWithZeroIp)
    WriteToS3.write("8. average number of transactions per block :" + numBlocks.toDouble / numTrans)
    WriteToS3.close()
  }

}

