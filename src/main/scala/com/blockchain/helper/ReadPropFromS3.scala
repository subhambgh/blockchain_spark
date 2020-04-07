package com.blockchain.helper

import java.util.Properties

import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.model.{GetObjectRequest, S3Object}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}

object ReadPropFromS3 {

  val s3Client: AmazonS3 = AmazonS3ClientBuilder.standard.withRegion(Regions.US_EAST_1).build
  val s3Object: S3Object  = s3Client.getObject(
    new GetObjectRequest("blockchain-bucket", "data/config-AWS.properties"))
  val prop = new Properties()
  prop.load(s3Object.getObjectContent())
  def getProperties(key:String) : String  = {
    return prop.getProperty(key)
  }
}
