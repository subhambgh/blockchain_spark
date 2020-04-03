package com.blockchain.app

import com.blockchain.helper.{ReadLastLine, ReadProperties, Writer}
import org.apache.spark.sql.functions.{col, max}
import org.apache.spark.sql.{Encoders, SparkSession}

object BlockChain2 {

  case class TxnSchema(txID: Int, blockID: Int, n_inputs: Int, n_outputs: Int)
  case class TxnHashSchema(txID: Int, hash: String)

  def main(args: Array[String]): Unit = {

    val sumTotalOpTrans:BigDecimal = BigDecimal(args(0))
    val numAddress:BigDecimal = BigDecimal(args(1))

    val spark = SparkSession
      .builder()
      .appName("BlockChain2")
      //.config("spark.master", "local")
      .getOrCreate()

    /*
    5. What is the transaction that has the greatest number of inputs? How
    many inputs exactly? Show the hash of that transaction. If there are
    multiple transactions that have the same greatest number of inputs, show
    all of them.  */

    var txnSchema = Encoders.product[TxnSchema].schema
    var txnDf = spark.read.format("csv").
      option("header", "false").
      option("delimiter", "\t").
      schema(txnSchema).
      load(ReadProperties.getProperties("tx")).
      cache()

    var txnHashSchema = Encoders.product[TxnHashSchema].schema
    var txnHashDf = spark.read.format("csv").
      option("header", "false").
      option("delimiter", "\t").
      schema(txnHashSchema).
      load(ReadProperties.getProperties("txh"))

    //val numTrans: BigDecimal = txnDf.count()
    val numTrans: BigDecimal  = BigDecimal(ReadLastLine.readLastLine(ReadProperties.getProperties("tx")))

    /*7. How many coinbase transactions are there in the dataset? */
    var numTransWithZeroIp = txnDf.select(col("txID")).where(col("n_inputs") === 0).count()

    txnDf = txnDf.select(txnDf.col("*")).where(col("n_inputs")=== txnDf.select(max("n_inputs")).first().get(0))

    var finalTxDf = txnDf.as("txnDf").join(txnHashDf.as("txnHashDf"), txnDf("txID") === txnHashDf("txID"), "inner").
      select(col("txnDf.*"), col("txnHashDf.hash"))
    var maxIp = finalTxDf.collectAsList().toString


    /* 6. What is the average transaction value? Transaction value is the sum of
    all outputs’ value.
    8. What is the average number of transactions per block?
     */
    val numBlocks: BigDecimal  = BigDecimal(ReadLastLine.readLastLine(ReadProperties.getProperties("bh")))

    Writer.write("1. number of transactions: " + numTrans + " & addresses:" + numAddress)
    Writer.write("5." + "\n" + maxIp)
    Writer.write("6. average transaction value :" + sumTotalOpTrans / numTrans)
    Writer.write("7. Number of coinbase transactions :" + numTransWithZeroIp)
    Writer.write("8. average number of transactions per block :" + numBlocks / numTrans)
    Writer.close()
  }

}

