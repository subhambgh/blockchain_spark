package com.blockchain.app

import com.blockchain.app.Part1_2.TxnHashSchema
import com.blockchain.helper.{ReadFromHDFS, ReadPropFromS3, WriteToS3}
import org.apache.spark.sql.functions.{col, count, desc, lit, max, sum, when}
import org.apache.spark.sql.{Encoders, SparkSession}

/**
 * things in here are similar to Part1_2
 * please refer to the descriptions there
 * */
object Part2 {

  case class TxnIpSchema(txID: Int, input_seq: Int, prev_txID: Int, prev_output_seq: Int, addrID: Int, sum: Long)
  case class TxnOpSchema(txID: Int, output_seq: Int, addrID: Int, sum: Long)
  case class AddressSchema(addrID: Int, userID: Int)
  case class TxnHashSchema(txID: Int, hash: String)

  def main(args: Array[String]): Unit = {

    val numAddress  = ReadFromHDFS.readNumLineFromHDFS(ReadPropFromS3.getProperties("readLinesAdd"))
    val sumTotalOpTrans  = args(0).toLong

    val spark = SparkSession
      .builder()
      .appName("Part2")
      .getOrCreate()
    val sqlContext = spark.sqlContext

    val txnIpSchema = Encoders.product[TxnIpSchema].schema
    val txnIpDf = spark.read.format("csv").
      option("header", "false").
      option("delimiter", "\t").
      schema(txnIpSchema).
      load(ReadPropFromS3.getProperties("txin")).
      select("txID","addrID", "sum")

    val txnOpSchema = Encoders.product[TxnOpSchema].schema
    val txnOpDf = spark.read.format("csv").
      option("header", "false").
      option("delimiter", "\t").
      schema(txnOpSchema).
      load(ReadPropFromS3.getProperties("txout")).
      select("txID","addrID", "sum")

    val addrSchema = Encoders.product[AddressSchema].schema
    val addDf = spark.read.format("csv").
      option("header", "false").
      option("delimiter", "\t").
      schema(addrSchema).
      load(ReadPropFromS3.getProperties("jcsc")).
      cache()

    val txnHashSchema = Encoders.product[TxnHashSchema].schema
    val txnHashDf = spark.read.format("csv").
      option("header", "false").
      option("delimiter", "\t").
      schema(txnHashSchema).
      load(ReadPropFromS3.getProperties("txh"))

    val numUsers = addDf.select("userID").distinct().count()

    val txnIpDfWithUserInfo =txnIpDf.as("txnIpDf").join(addDf.as("addDf"),
      txnIpDf("addrID")===addDf("addrID"),"inner")
      .select(col("txnIpDf.addrID").as("addrID")
        ,col("txnIpDf.txID").as("txID")
        ,col("txnIpDf.sum").as("sum")
        ,col("addDf.userID").as("userID"))

    val txnOpDfWithUserInfo =txnOpDf.as("txnOpDf").join(addDf.as("addDf"),
      txnOpDf("addrID")===addDf("addrID"),"inner")
      .select(col("txnOpDf.addrID").as("addrID")
        ,col("txnOpDf.txID").as("txID")
        ,col("txnOpDf.sum").as("sum")
        ,col("addDf.userID").as("userID"))

    val groupByUsrIpDf = txnIpDfWithUserInfo.groupBy(col("userID"))
      .agg(sum("sum").as("utxoIp"))

    val groupByUsrOpDf = txnOpDfWithUserInfo.groupBy(col("userID"))
      .agg(sum("sum").as("utxoOp"))

    val txnUtxoDf = groupByUsrIpDf.as("groupByUsrIpDf").join(groupByUsrOpDf.as("groupByUsrOpDf"),
          groupByUsrIpDf("userID") === groupByUsrOpDf("userID"), "full_outer")
      .select(col("groupByUsrOpDf.userID").as("outUserID"),
        col("groupByUsrIpDf.userID").as("inUserID"),
        col("groupByUsrOpDf.utxoOp").as("utxoOp"),
        col("groupByUsrIpDf.utxoIp").as("utxoIp"))

    val finalTxnUtxoDf = txnUtxoDf.withColumn("utxo",
      when(col("utxoOp").isNull, lit(-1)*col("utxoIp")).otherwise(
        when(col("utxoIp").isNull,col("utxoOp")).otherwise(
          col("utxoOp") - col("utxoIp")
        )))
      .withColumn("userID", when( col("outUserID").isNull,
        col("inUserID")).otherwise(col("outUserID")))
      .cache()

    val maxAdd_maxSum = finalTxnUtxoDf.orderBy(desc("utxo")).first()
    val totalBalance = finalTxnUtxoDf.select(sum("utxo").as("totalBalance")).
      first().getAs[Long]("totalBalance")

    //clear cache
    sqlContext.clearCache()
    /**
     * */
    val groupByTransUsrIpDf = txnIpDfWithUserInfo.groupBy(col("txID"),col("userID"))
      .agg(count(lit(1)).as("ipCount"))
      .cache()
    val groupByTransUsrOpDf = txnOpDfWithUserInfo.groupBy(col("txID"),col("userID"))
      .agg(count(lit(1)).as("opCount"), sum("sum").as("opSum"))
      .cache()

    val numIpTransPerUsr = groupByTransUsrIpDf.count()
    val numOpTransPerUsr = groupByTransUsrOpDf.count()

    val txSendingMaxBitCoin = groupByTransUsrOpDf
      .select(col("txID").as("txID"),
        max(col("opSum")).as("maxSum"))
      .where(col("userID")===maxAdd_maxSum.getAs[Long]("userID"))

    val txSendingMaxBitCoinWithHash = txSendingMaxBitCoin
      .join(txnHashDf.as("txnHashDf"), txSendingMaxBitCoin("txID") === txnHashDf("txID"), "inner").
      select(col("txSendingMaxBitCoin.*"), col("txnHashDf.hash").as("hash"))
        .first()

    WriteToS3.write("numUsers="+numUsers)
    WriteToS3.write("numIpTransPerUsr="+numIpTransPerUsr)
    WriteToS3.write("numOpTransPerUsr="+numOpTransPerUsr)
    //WriteToS3.write("numTxnCountPerUsr="+numTxnCountPerUsr)
    WriteToS3.write("2. "+ maxAdd_maxSum.getAs[Long]("userID")
      + " : user has greatest amount of bitcoin= " + maxAdd_maxSum.getAs[Long]("utxo"))
    WriteToS3.write("3. avg balance/user: " + totalBalance.toDouble/numUsers)
    WriteToS3.write("4.1. total number of i/p transactions/user : " + numIpTransPerUsr.toDouble/numUsers
      + " & o/p transactions/address : " + numOpTransPerUsr.toDouble/numUsers)
    WriteToS3.write("number of users: "+ numUsers+" & sum of total output transaction amounts: "+sumTotalOpTrans)
    WriteToS3.write("transaction sending max bitcoin: " +
      "" +txSendingMaxBitCoinWithHash.getAs[Long]("hash"))
    WriteToS3.close()

  }

}

