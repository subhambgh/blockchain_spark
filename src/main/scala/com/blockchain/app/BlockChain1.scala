package com.blockchain.app

import com.blockchain.helper.{ReadFromHDFS, ReadPropFromS3, WriteToS3}
import org.apache.spark.sql.functions.{col, desc, sum}
import org.apache.spark.sql.{Encoders, SparkSession}

object BlockChain1 {

  case class TxnIpSchema(txID: Int, input_seq: Int, prev_txID: Int, prev_output_seq: Int, addrID: Int, sum: BigInt)
  case class TxnOpSchema(txID: Int, output_seq: Int, addrID: Int, sum: BigInt)

  def main(args: Array[String]): Unit = {

    val numAddress: BigDecimal  = BigDecimal(ReadFromHDFS.readLastLineFromHDFS(ReadPropFromS3.getProperties("add")))
    val numIpTrans: BigDecimal  = BigDecimal(ReadFromHDFS.readNumLineFromHDFS(ReadPropFromS3.getProperties("txin")))

    val spark = SparkSession
      .builder()
      .appName("BlockChain1")
      .getOrCreate()
    val sqlContext = spark.sqlContext

    val txnIpSchema = Encoders.product[TxnIpSchema].schema
    val txnIpDf = spark.read.format("csv").
      option("header", "false").
      option("delimiter", "\t").
      schema(txnIpSchema).
      load(ReadPropFromS3.getProperties("txin")).
      select("addrID", "sum")

    val txnOpSchema = Encoders.product[TxnOpSchema].schema
    val txnOpDf = spark.read.format("csv").
      option("header", "false").
      option("delimiter", "\t").
      schema(txnOpSchema).
      load(ReadPropFromS3.getProperties("txout")).
      select( "addrID", "sum")
//
//    val txnIpUtxoDf = txnIpDf.as("txnIpDf1").join(txnIpDf.as("txnIpDf2"),
//      col("txnIpDf1.txID") === col("txnIpDf2.prev_txID"), "left_anti").
//      dropDuplicates("txID")
//
//    val txnIpUtxoDf_txnOpDf = txnIpUtxoDf.as("txnIpUtxoDf").join(txnOpDf.as("txnOpDf"),
//      txnIpUtxoDf("txID") === txnOpDf("txID"), "inner").
//      select(col("txnIpUtxoDf.*"), col("txnOpDf.addrID"), col("txnOpDf.sum"))
//
//    val finalDf = txnIpUtxoDf_txnOpDf.groupBy(col("addrID")).agg(sum("sum").as("utxo")).
//      orderBy(desc("utxo")).cache()
//
//    val maxAdd_maxSum = finalDf.limit(1).first()

    val groupByAddIpDf = txnIpDf.groupBy(col("addrID")).agg(sum("sum").as("utxoIp"))
    val groupByAddOpDf = txnOpDf.groupBy(col("addrID")).agg(sum("sum").as("utxoOp"))
    val txnUtxoDf = groupByAddIpDf.as("groupByAddIpDf").join(groupByAddOpDf.as("groupByAddOpDf"),
          groupByAddIpDf("addrID") === groupByAddOpDf("addrID"), "right_outer")
      .select(col("groupByAddOpDf.addrID").as("addrID"),
        (col("groupByAddOpDf.utxoOp")-col("groupByAddIpDf.utxoIp")).as("utxo"))
      .cache()

    val maxAdd_maxSum = txnUtxoDf.orderBy(desc("utxo")).first()

    /* 3. What is the average balance per address? */
    val totalBalance = BigDecimal(txnUtxoDf.select(sum("utxo").as("totalBalance")).
      first().getAs("totalBalance").toString)

    txnOpDf.createOrReplaceTempView("txnOpDf")
    val numOpTrans_sumTotalOpTrans = sqlContext.sql("SELECT count(1) AS count,sum(sum) AS opsum FROM txnOpDf").first()
    val numOpTrans:BigDecimal = BigDecimal(numOpTrans_sumTotalOpTrans.getAs("count").toString)
    val sumTotalOpTrans:BigDecimal = BigDecimal(numOpTrans_sumTotalOpTrans.getAs("opsum").toString)

    /*1. What is the number of transactions and addresses in the dataset?*/
    //Writer.write("number of transactions: " + numTrans + " & addresses:" + numAddress);

    /*2. What is the Bitcoin address that is holding the greatest amount of bitcoins?
        How much is that exactly? Note that the address here must be
        a valid Bitcoin address string. To answer this, you need to calculate the
        balance of each address. The balance here is the total amount of bitcoins
        in the UTXOs of an address.*/
    /*
    4. What is the average number of input and output transactions per address?
    What is the average number of transactions per address (including both
    inputs and outputs)? An output transaction of an address is the transaction
    that is originated from that address. Likewise, an input transaction
    of an address is the transaction that sends bitcoins to that address.
    */

    WriteToS3.write("2. "+ maxAdd_maxSum.getAs[BigInt]("addrID")
      + " : address has greatest amount of bitcoin= " + maxAdd_maxSum.getAs[BigDecimal]("utxo"))
    WriteToS3.write("3. avg balance/address: " + totalBalance/numAddress)
    WriteToS3.write("4.1. total number of i/p transactions/address : " + numIpTrans/numAddress
      + " & o/p transactions/address : " + numOpTrans/numAddress)
    WriteToS3.write("number of address: "+ numAddress+" & sum of total output transaction amounts: "+sumTotalOpTrans)
    WriteToS3.close()

  }


}

