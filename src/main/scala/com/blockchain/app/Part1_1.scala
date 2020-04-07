package com.blockchain.app

import com.blockchain.helper.{ReadFromHDFS, ReadPropFromS3, WriteToS3}
import org.apache.spark.sql.functions.{col, desc, sum,lit, when}
import org.apache.spark.sql.{Encoders, SparkSession}

object Part1_1 {

  case class TxnIpSchema(txID: Int, input_seq: Int, prev_txID: Int, prev_output_seq: Int, addrID: Int, sum: BigInt)
  case class TxnOpSchema(txID: Int, output_seq: Int, addrID: Int, sum: BigInt)

  def main(args: Array[String]): Unit = {

    val numAddress: BigDecimal  = BigDecimal(ReadFromHDFS.readNumLineFromHDFS(ReadPropFromS3.getProperties("readLinesAdd")))

    val spark = SparkSession
      .builder()
      .appName("Part1_1")
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
    val totalBalance = BigDecimal(finalTxnUtxoDf.select(sum("utxo").as("totalBalance")).
      first().getAs("totalBalance").toString)


    /**
     * now to calculate the total number of I/p and O/p transactions
     * remember, txin can have multiple rows for a single transactions - so we can't directly count the num rows
     * So, we group by trans id first and then count the num rows
     *
     * */
      //ip
    val groupByTransIpDf = txnIpDf.groupBy(col("txID")).agg(sum("sum").as("txIpSum"))
    groupByTransIpDf.createOrReplaceTempView("groupByTransIpDf")
    val numIpTrans = BigDecimal(sqlContext.sql(
      "SELECT count(1) AS count FROM groupByTransIpDf ").first().getAs("count").toString)
      //op
    val groupByTransOpDf = txnOpDf.groupBy(col("txID")).agg(sum("sum").as("txOpSum"))
    groupByTransOpDf.createOrReplaceTempView("groupByTransOpDf")
    val numOpTrans_sumTotalOpTrans = sqlContext.sql(
      "SELECT count(1) AS count,sum(txOpSum) AS opsum FROM groupByTransOpDf ").first()
    val numOpTrans:BigDecimal = BigDecimal(numOpTrans_sumTotalOpTrans.getAs("count").toString)
    val sumTotalOpTrans:BigDecimal = BigDecimal(numOpTrans_sumTotalOpTrans.getAs("opsum").toString)

    /**
     * write files to s3
     *
     * */
    WriteToS3.write("totalBalance="+totalBalance)
    WriteToS3.write("numAddress="+numAddress)
    WriteToS3.write("numIpTrans="+numIpTrans)
    WriteToS3.write("numOpTrans="+numOpTrans)
    WriteToS3.write("sumTotalOpTrans="+sumTotalOpTrans)

    WriteToS3.write("2. "+ maxAdd_maxSum.getAs[BigInt]("addrID")
      + " : address has greatest amount of bitcoin= " + maxAdd_maxSum.getAs[BigDecimal]("utxo"))
    WriteToS3.write("3. avg balance/address: " + totalBalance/numAddress)
    WriteToS3.write("4.1. total number of i/p transactions/address : " + numIpTrans/numAddress
      + " & o/p transactions/address : " + numOpTrans/numAddress)
    WriteToS3.write("number of address: "+ numAddress+" & sum of total output transaction amounts: "+sumTotalOpTrans)
    WriteToS3.close()

  }


}

