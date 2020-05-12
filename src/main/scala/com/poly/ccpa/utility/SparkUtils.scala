package com.poly.ccpa.utility

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.regions.Regions
import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagementClientBuilder
import com.amazonaws.services.simplesystemsmanagement.model.{GetParametersRequest, GetParametersResult}
import com.poly.utils.DDB
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

import scala.collection.JavaConverters._

class SparkUtils(sc: SparkContext, stringBuilder: java.lang.StringBuilder) extends java.io.Serializable {
  val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
  val date = format.format(new java.util.Date())
  val partitions = Runtime.getRuntime.availableProcessors() * 9

  def orcWriter(target: String, df: DataFrame): Unit = {
    try {
      df
        .coalesce(1)
        .write
        .mode(SaveMode.Overwrite)
        .option("compression", "snappy")
        .option("orc.create.index", "true")
        .format("orc")
        .save(target)
    }
    catch {
      case e: Exception => {
        println("Exception", e)
        stringBuilder.append("ERROR " + e.getMessage).append("\n")
      }
    }
  }

  def gzipWriter(target: String, df: DataFrame): Unit = {
    try {
      df
        .coalesce(1)
        .write
        .mode(SaveMode.Overwrite)
        .format("csv")
        .option("delimiter", "\t")
        .option("header", "false")
        .option("quoteAll", "true")
        .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
        .save(target)
    }
    catch {
      case e: Exception => {
        println("Exception", e)
        stringBuilder.append("ERROR " + e.getMessage).append("\n")
      }
    }
  }

  def ddbRead(sql: SQLContext, tableName: String): DataFrame = {

    /*use https://github.com/audienceproject/spark-dynamodb to convert ddb table to dataframe*/
    import sql.implicits._
    val userDDB: DDB = new DDB(stringBuilder)
    val mapper = userDDB.parallelScan(tableName).asScala.toMap;
    stringBuilder.append("INFO id service dynamoDB count: " + intFormatter(mapper.size)).append("\n")
    println("INFO id service dynamoDB count: " + intFormatter(mapper.size))
    val resultRDD: RDD[(String, String)] = sc.parallelize(mapper.toSeq)
    resultRDD.toDF(new Schemas().idService().fieldNames: _*)
  }

  /*wrapped in a static object and broadcasted the DataFrame since foreachPartition does not serialize
  https://www.nicolaferraro.me/2016/02/22/using-non-serializable-objects-in-apache-spark/
   */
  object DBB {
    def ddbWrite(dataFrame: DataFrame): Unit = {
      val userDDB: DDB = new DDB()
      /*ddb id service
      * userDDB.getUUID("email-cgillis@cooley.com").get("email-cgillis@cooley.com")
      * userDDB.writeItem("gravy-1234")
      * */
      try {
        val staticDF = sc.broadcast(dataFrame)
        println("writing to DynamoDB")
        staticDF.value
          .repartition(partitions)
          .foreachPartition(partition => {
            partition.foreach(record => {
              userDDB.writeItem(record.getAs[String]("sourceSystemId"), record.getAs[String]("uuid"))
            })
          })
      }
      catch {
        case e: Exception => {
          println("Exception", e)
          stringBuilder.append("ERROR " + e.getMessage).append("\n")
          val trace = e.getStackTrace
          for (etrace <- trace) {
            println("Exception", etrace.toString)
            stringBuilder.append("ERROR " + etrace.toString).append("\n")
          }
        }
      }
    }
  }

  def dbWrite(hostParam: String, uidParam: String, pwParam: String, tableName: String, dataFrame: DataFrame): Unit = {
    try {
      /*https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html*/
      dataFrame
        .repartition(Runtime.getRuntime.availableProcessors() * 2)
        .write
        .format("jdbc")
        .option("url", hostParam)
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("dbtable", tableName)
        .option("user", uidParam)
        .option("password", pwParam)
        .option("batchsize", 5000)
        .mode(SaveMode.Append)
        .save()
    }
    catch {
      case e: Exception => {
        println("Exception", e)
        stringBuilder.append("ERROR " + e.getMessage).append("\n")
        val trace = e.getStackTrace
        for (etrace <- trace) {
          println("Exception", etrace.toString)
          stringBuilder.append("ERROR " + etrace.toString).append("\n")
        }
      }
    }
  }

  def getSSMParam(param: String): String = {
    val cli = AWSSimpleSystemsManagementClientBuilder
      .standard
      .withRegion(Regions.US_WEST_2)
      .withCredentials(new DefaultAWSCredentialsProviderChain)
      .build

    val request: GetParametersRequest = new GetParametersRequest
    request.withWithDecryption(true).withNames(param)
    val result: GetParametersResult = cli.getParameters(request)
    result.getParameters.get(0).getValue
  }

  def intFormatter(value: Any): String = {
    val formatter = java.text.NumberFormat.getIntegerInstance
    formatter.format(value)
  }

}



