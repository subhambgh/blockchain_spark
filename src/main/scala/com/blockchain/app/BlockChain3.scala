package com.blockchain.app

import com.blockchain.helper.ReadProperties
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Encoders, SparkSession}

object BlockChain3 {

  case class TxnIpSchema(txID: Int, input_seq: Int, prev_txID: Int, prev_output_seq: Int, addrID: Int, sum: BigInt)
  case class AddressSchema(addrID: Int, address: String)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("BlockChain2")
      .config("spark.master", "local")
      .getOrCreate()

    val txnIpSchema = Encoders.product[TxnIpSchema].schema
    val txnIpDf = spark.read.format("csv")
      .option("header", "false")
      .option("delimiter", "\t")
      .option("io.compression.codecs", "io.sensesecure.hadoop.xz.XZCodec")
      .schema(txnIpSchema)
      .load(ReadProperties.getProperties("txin"))

    val addSchema = Encoders.product[AddressSchema].schema
    val addDf = spark.read.format("csv").
          option("header", "false").
          option("delimiter", "\t").
          schema(addSchema).
          load(ReadProperties.getProperties("add"))
        val numAddress: BigDecimal = addDf.count()


  }

}

