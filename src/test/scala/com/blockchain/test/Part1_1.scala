package com.blockchain.test

import com.blockchain.helper.{ReadFromHDFS, ReadPropFromS3, WriteToS3}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Encoders, SparkSession}

object Part1_1 {

  case class TxnIpSchema(txID: Int, input_seq: Int, prev_txID: Int, prev_output_seq: Int, addrID: Int, sum: Long)
  case class TxnOpSchema(txID: Int, output_seq: Int, addrID: Int, sum: Long)

  def main(args: Array[String]): Unit = {

    val numAddress = 1//ReadFromHDFS.readNumLineFromHDFS(ReadPropFromS3.getProperties("readLinesAdd"))

    val spark = SparkSession
      .builder()
      .appName("Part1_1")
      .config("spark.master", "local")
      .getOrCreate()
    val sqlContext = spark.sqlContext

    val txnIpSchema = Encoders.product[TxnIpSchema].schema
    val txnIpDf = spark.read.format("csv").
      option("header", "false").
      option("delimiter", "\t").
      schema(txnIpSchema).
      load("E:/hw2/local/txin1.txt").
      select("txID","addrID", "sum")

    val txnOpSchema = Encoders.product[TxnOpSchema].schema
    val txnOpDf = spark.read.format("csv").
      option("header", "false").
      option("delimiter", "\t").
      schema(txnOpSchema).
      load("E:/hw2/local/txout.txt").
      select( "txID","addrID", "sum")

    /**
     * UTXO for an addresses is calculated as: (sum of amount out) - (sum of amount in) to that address
     *
     * */
    val groupByAddIpDf = txnIpDf.groupBy(col("addrID")).agg(sum("sum").as("utxoIp"))
    val groupByAddOpDf = txnOpDf.groupBy(col("addrID")).agg(sum("sum").as("utxoOp"))
    val txnUtxoDf = groupByAddIpDf.as("groupByAddIpDf").join(groupByAddOpDf.as("groupByAddOpDf"),
          groupByAddIpDf("addrID") === groupByAddOpDf("addrID"), "full_outer")
      .select(col("groupByAddOpDf.addrID").as("outAddrID"),
        col("groupByAddIpDf.addrID").as("inAddrID"),
        col("groupByAddOpDf.utxoOp").as("utxoOp"),
        col("groupByAddIpDf.utxoIp").as("utxoIp"))

    /**
     * case 1. if add is not present in txout - replace address with txin address
     * case 2. in above case, when an add isn't present
     *        - its utxo will be null, thus, replace with groupByAddOpDf.utxoOp
     * case 3. if balance is negative - cannot be the case
     * */
    val finalTxnUtxoDf = txnUtxoDf.withColumn("utxo",
      when(col("utxoOp").isNull, lit(-1)*col("utxoIp")).otherwise(
        when(col("utxoIp").isNull,col("utxoOp")).otherwise(
          col("utxoOp") - col("utxoIp")
        )))
        .withColumn("addrID", when( col("outAddrID").isNull,
      col("inAddrID")).otherwise(col("outAddrID")))
      .cache()
    val maxAdd_maxSum = finalTxnUtxoDf.orderBy(desc("utxo")).first()
    val totalBalance = (finalTxnUtxoDf.select(sum("utxo").as("totalBalance")).
      first().getAs[Long]("totalBalance"))

    //clear cache
    sqlContext.clearCache()

      //op
    val groupByTransOpDf = txnOpDf.groupBy(col("txID")).agg(sum("sum").as("txOpSum"))
    groupByTransOpDf.createOrReplaceTempView("groupByTransOpDf")
    val numOpTrans_sumTotalOpTrans = sqlContext.sql(
      "SELECT count(1) AS count,sum(txOpSum) AS opsum FROM groupByTransOpDf ").first()
    val sumTotalOpTrans = numOpTrans_sumTotalOpTrans.getAs[Long]("opsum")

    /**
     * now to calculate the total number of I/p and O/p transactions
     * remember, txin can have multiple rows for a single transactions - so we can't directly count the num rows
     * So, we group by trans id first and then count the num rows
     *
     * Anyhow, a single transaction can have multiple i/p from same address
     * so, to calculate avg no. of i/p transactions per address
     *  we have to group by txID, addrID (together) - and then count the number of rows
     * */

    val groupByTransAddIpDf = txnIpDf.groupBy(col("txID"),col("addrID"))
      .agg(count(lit(1)).as("ipCount"))

    val groupByTransAddOpDf = txnOpDf.groupBy(col("txID"),col("addrID"))
      .agg(count(lit(1)).as("opCount"))

    val numIpTransPerAdd = groupByTransAddIpDf.count()
    val numOpTransPerAdd = groupByTransAddOpDf.count()

//    val txnCountDf = groupByTransAddIpDf.as("groupByTransAddIpDf")
//      .join(groupByTransAddOpDf.as("groupByTransAddOpDf"),
//      groupByTransAddIpDf("addrID") === groupByTransAddOpDf("addrID"), "full_outer")
//      .select(col("groupByTransAddIpDf.ipCount").as("ipCount"),
//        col("groupByTransAddOpDf.opCount").as("opCount"))
//
//    val numTxnCountPerAdd = txnCountDf.withColumn("tCount",
//      when(col("opCount").isNull, lit(0))
//        + when(col("ipCount").isNull, lit(0)))
//        .select(col("tCount")).rdd.map(_(0).asInstanceOf[Long]).reduce(_+_)

    /**
     * write files to s3
     *
     * */
    println("totalBalance="+totalBalance)
    println("numAddress="+numAddress)
    println("numIpTransPerAdd="+numIpTransPerAdd)
    println("numOpTransPerAdd="+numOpTransPerAdd)
    println("sumTotalOpTrans="+sumTotalOpTrans)
    //println("numTxnCountPerAdd="+numTxnCountPerAdd)
    println("2. "+ maxAdd_maxSum.getAs[Long]("addrID")
      + " : address has greatest amount of bitcoin= " + maxAdd_maxSum.getAs[Long]("utxo"))
    println("3. avg balance/address: " + totalBalance.toDouble/numAddress)
    println("4.1. total number of i/p transactions/address : " + numIpTransPerAdd.toDouble/numAddress
      + " & o/p transactions/address : " + numOpTransPerAdd.toDouble/numAddress)
    println("number of address: "+ numAddress+" & sum of total output transaction amounts: "+sumTotalOpTrans)

  }


}

