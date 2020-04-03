package com.blockchain.app

import com.blockchain.helper.{ReadLastLine, ReadProperties, Writer}
import org.apache.spark.sql.functions.{col, desc, sum}
import org.apache.spark.sql.{Encoders, SparkSession}

object BlockChain1 {

  case class TxnIpSchema(txID: Int, input_seq: Int, prev_txID: Int, prev_output_seq: Int, addrID: Int, sum: BigInt)
  case class TxnOpSchema(txID: Int, output_seq: Int, addrID: Int, sum: BigInt)
  case class AddressSchema(addrID: Int, address: String)

  def main(args: Array[String]): Unit = {
    /*1. What is the number of transactions and addresses in the dataset?*/
    //Writer.write("number of transactions: " + numTrans + " & addresses:" + numAddress);

    /*2. What is the Bitcoin address that is holding the greatest amount of bitcoins?
        How much is that exactly? Note that the address here must be
        a valid Bitcoin address string. To answer this, you need to calculate the
        balance of each address. The balance here is the total amount of bitcoins
        in the UTXOs of an address.*/
    val spark = SparkSession
      .builder()
      .appName("BlockChain1")
      //.config("spark.master", "local")
      .getOrCreate()
    val sqlContext = spark.sqlContext

//    val addSchema = Encoders.product[AddressSchema].schema
//    val addDf = spark.read.format("csv").
//      option("header", "false").
//      option("delimiter", "\t").
//      schema(addSchema).
//      load(ReadProperties.getProperties("add"))
//    val numAddress: BigDecimal = addDf.count()
    val numAddress: BigDecimal  = BigDecimal(ReadLastLine.readLastLine(ReadProperties.getProperties("add")))

    val txnIpSchema = Encoders.product[TxnIpSchema].schema
    val txnIpDf = spark.read.format("csv").
      option("header", "false").
      option("delimiter", "\t").
      schema(txnIpSchema).
      load(ReadProperties.getProperties("txin")).
      select("addrID", "sum")
//    val numIpTrans: BigDecimal = txnIpDf.count()
    val numIpTrans: BigDecimal  = BigDecimal(ReadLastLine.readLastLine(ReadProperties.getProperties("txin")))

    val txnOpSchema = Encoders.product[TxnOpSchema].schema
    val txnOpDf = spark.read.format("csv").
      option("header", "false").
      option("delimiter", "\t").
      schema(txnOpSchema).
      load(ReadProperties.getProperties("txout")).
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
        (col("groupByAddOpDf.sum")-col("groupByAddIpDf.sum")).as("utxo"))
      .cache()

    val maxAdd_maxSum = txnUtxoDf.orderBy(desc("utxo")).first()

    /* 3. What is the average balance per address? */
    val totalBalance = BigDecimal(txnUtxoDf.select(sum("utxo").as("totalBalance")).
      first().getAs("totalBalance").toString)

    /*
    4. What is the average number of input and output transactions per address?
    What is the average number of transactions per address (including both
    inputs and outputs)? An output transaction of an address is the transaction
    that is originated from that address. Likewise, an input transaction
    of an address is the transaction that sends bitcoins to that address.
    */

    txnOpDf.createOrReplaceTempView("txnOpDf")
    val numOpTrans_sumTotalOpTrans = sqlContext.sql("SELECT count(1) AS count,sum(sum) AS opsum FROM txnOpDf").first()
    val numOpTrans:BigDecimal = BigDecimal(numOpTrans_sumTotalOpTrans.getAs("count").toString)
    val sumTotalOpTrans:BigDecimal = BigDecimal(numOpTrans_sumTotalOpTrans.getAs("opsum").toString)

    Writer.write("2. "+ maxAdd_maxSum.getAs[BigInt]("addrID")
      + " : address has greatest amount of bitcoin= " + maxAdd_maxSum.getAs[BigDecimal]("utxo"))
    Writer.write("3. avg balance/address: " + totalBalance/numAddress)
    Writer.write("4.1. total number of i/p transactions/address : " + numIpTrans/numAddress
      + " & o/p transactions/address : " + numOpTrans/numAddress)
    Writer.write("4.2. total number of transactions/address : " + (numIpTrans+numOpTrans) / numAddress)
    Writer.write("number of address: "+ numAddress+" & sum of total output transaction amounts: "+sumTotalOpTrans)
    Writer.close()

  }

}

