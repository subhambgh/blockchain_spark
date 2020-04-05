package com.blockchain.app

import com.blockchain.helper.{ReadFromHDFS, ReadPropFromS3, WriteToS3}
import org.apache.spark.sql.functions.{col, desc, sum}
import org.apache.spark.sql.{Encoders, SparkSession}

object BlockChain4 {

  case class TxnIpSchema(txID: Int, input_seq: Int, prev_txID: Int, prev_output_seq: Int, addrID: Int, sum: BigInt)
  case class TxnOpSchema(txID: Int, output_seq: Int, addrID: Int, sum: BigInt)
  case class AddressSchema(addrID: Int, userID: Int)

  def main(args: Array[String]): Unit = {

    val numAddress: BigDecimal  = BigDecimal(ReadFromHDFS.readLastLineFromHDFS(ReadPropFromS3.getProperties("add")))
    val numIpTrans: BigDecimal  = BigDecimal(ReadFromHDFS.readNumLineFromHDFS(ReadPropFromS3.getProperties("txin")))
    val numOpTrans: BigDecimal  = BigDecimal(args(0))
    val sumTotalOpTrans: BigDecimal  = BigDecimal(args(1))

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

    val addrSchema = Encoders.product[AddressSchema].schema
    val addDf = spark.read.format("csv").
      option("header", "false").
      option("delimiter", "\t").
      schema(addrSchema).
      load(ReadPropFromS3.getProperties("jcsc")).
      cache()

    val numUsers:BigDecimal = BigDecimal(addDf.select("userID").count())

    val txnIpDfWithUserInfo =txnIpDf.as("txnIpDf").join(addDf.as("addDf"),
      txnIpDf("addrID")===addDf("addrID"),"inner")
      .select(col("txnIpDf.addrID").as("addrID"),col("txnIpDf.sum").as("sum")
        ,col("addDf.userID").as("userID"))

    val txnOpDfWithUserInfo =txnOpDf.as("txnOpDf").join(addDf.as("addDf"),
      txnOpDf("addrID")===addDf("addrID"),"inner")
      .select(col("txnOpDf.addrID").as("addrID"),col("txnOpDf.sum").as("sum")
        ,col("addDf.userID").as("userID"))

    val groupByUsrIpDf = txnIpDfWithUserInfo.groupBy(col("userID"))
      .agg(sum("sum").as("utxoIp"))

    val groupByUsrOpDf = txnOpDfWithUserInfo.groupBy(col("userID"))
      .agg(sum("sum").as("utxoOp"))

    val txnUtxoDf = groupByUsrIpDf.as("groupByUsrIpDf").join(groupByUsrOpDf.as("groupByUsrOpDf"),
          groupByUsrIpDf("userID") === groupByUsrOpDf("userID"), "right_outer")
      .select(col("groupByUsrOpDf.userID").as("userID"),
        (col("groupByUsrOpDf.utxoOp")-col("groupByUsrIpDf.utxoIp")).as("utxo"))
      .cache()

    val maxAdd_maxSum = txnUtxoDf.orderBy(desc("utxo")).first()

    val totalBalance = BigDecimal(txnUtxoDf.select(sum("utxo").as("totalBalance")).
      first().getAs("totalBalance").toString)


    /*2. What is the Bitcoin address that is holding the greatest amount of bitcoins?
        How much is that exactly? Note that the address here must be
        a valid Bitcoin address string. To answer this, you need to calculate the
        balance of each address. The balance here is the total amount of bitcoins
        in the UTXOs of an address.
        3. What is the average balance per address?
        4. What is the average number of input and output transactions per address?
        What is the average number of transactions per address (including both
        inputs and outputs)? An output transaction of an address is the transaction
        that is originated from that address. Likewise, an input transaction
        of an address is the transaction that sends bitcoins to that address.
     */

    WriteToS3.write("2. "+ maxAdd_maxSum.getAs[BigInt]("userID")
      + " : user has greatest amount of bitcoin= " + maxAdd_maxSum.getAs[BigDecimal]("utxo"))
    WriteToS3.write("3. avg balance/user: " + totalBalance/numUsers)
    WriteToS3.write("4.1. total number of i/p transactions/user : " + numIpTrans/numUsers
      + " & o/p transactions/address : " + numOpTrans/numUsers)
    WriteToS3.write("number of users: "+ numUsers+" & sum of total output transaction amounts: "+sumTotalOpTrans)
    WriteToS3.close()

  }

}

