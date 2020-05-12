package com.poly.ccpa.deletor

import java.util.concurrent._

import com.poly.utils._
import com.poly.ccpa.utility._
import org.apache.commons.lang3.time.StopWatch
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.storage.StorageLevel

class Customers extends java.io.Serializable {

  val config: ConfigProps = new ConfigProps
  val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
  val date = format.format(new java.util.Date())
  val sw = new StopWatch


  def delete(sparkSession: SparkSession, sc: SparkContext, sql: SQLContext, source: String, target: String, format: String, stringBuilder: java.lang.StringBuilder): Unit = {
    sw.start()
    /*sets checkpoint directory to track lineage in s3
    * Checkpointing is a process of truncating RDD lineage graph and saving it to a reliable distributed file system
    * */
    sc.setCheckpointDir("s3://polyglotDataNerd-bigdata-utility/spark/checkpoints/ccpa/")
    val schemas: Schemas = new Schemas
    val utils: SparkUtils = new SparkUtils(sc, stringBuilder)

    sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", utils.getSSMParam("/s3/polyglotDataNerd/admin/AccessKey"))
    sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", utils.getSSMParam("/s3/polyglotDataNerd/admin/SecretKey"))
    sc.hadoopConfiguration.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    sc.hadoopConfiguration.set("fs.s3.canned.acl", "BucketOwnerFullControl")
    sc.hadoopConfiguration.set("fs.s3n.enableServerSideEncryption", "true")
    sc.hadoopConfiguration.set("fs.s3n.serverSideEncryptionAlgorithm", "AES256")
    try {
      /*imports functions*/
      /**/ val sourceDF = sql
        .read
        .option("delimiter", "\t")
        .option("quote", "\"")
        .option("escape", "\"")
        .schema(schemas.gravyCustomers())
        .csv(source)
        .distinct()
        .persist(StorageLevel.MEMORY_ONLY_SER_2)
      sourceDF.createGlobalTempView("sourceView")

      val emails = sql
        .read
        .option("delimiter", "\t")
        .schema(schemas.emaildeletes())
        .csv("s3://polyglotDataNerd-bigdata-private/email_deletion_request/*")
        .distinct()
        .persist(StorageLevel.MEMORY_ONLY_SER_2)
      emails.createGlobalTempView("delete")
      stringBuilder.append("INFO number of emails in deletion directory:  " + emails.count()).append("\n")
      println(stringBuilder.toString())

      /**/ val writeDF = sparkSession.sql(
        """
          select distinct a.*
          from global_temp.sourceView a left join global_temp.delete b on trim(a.email) = lower(trim(b.email))
          where lower(trim(b.email)) is null
          """.stripMargin)
        .persist(StorageLevel.MEMORY_ONLY_SER_2)
      writeDF.createGlobalTempView("write")

      if (format.equals("orc")) {
        val sourceList = source.split("/").toList
        val bucket = sourceList(2)
        val prefix = sourceList(3) + "/" + sourceList(4) + "/" + sourceList(5)

        /*overwrite source after email has been removed in DataLake Anonymized*/
        utils.orcWriter(target + "/orc/deleteOverwrite_" + date + "/", sparkSession.sql("select * from write"))

        /*remove old version files*/
        new S3(bucket, prefix, stringBuilder, "us-west-2").removeOjectsDelete("/orc/deleteOverwrite_" + date + "/")
      }

      if (format.equals("gzip")) {
        val sourceList = source.split("/").toList
        val bucket = sourceList(2)
        val prefix = sourceList(3) + "/" + sourceList(4) + "/" + sourceList(5)

        /*overwrite source after email has been removed in DataLake Anonymized*/
        utils.gzipWriter(target + "/gzip/deleteOverwrite_" + date + "/", sparkSession.sql("select * from global_temp.write"))

        /*remove old version files*/
        new S3(bucket, prefix, stringBuilder, "us-west-2").removeOjectsDelete("/gzip/deleteOverwrite_" + date + "/")
      }

      sw.stop()
      stringBuilder.append("INFO gravy customers deletion runtime (seconds): " + sw.getTime(TimeUnit.SECONDS)).append("\n")
      println(stringBuilder.toString())

      new EmailWrapper(config.getPropValues("emails"), config.getPropValues("fromemails"),
        "ETL Notification " + " SPARK: " + " CCPA GRAVY DELETION " + source,
        "File Location Target: " + target + format + "/" + "deleteOverwrite_" + date +
          "\n" + stringBuilder.toString()).sendEMail()
    }
    catch {
      case e: Exception => {
        println("Exception", e)
        stringBuilder.append("ERROR " + e.getMessage).append("\n")
      }
    }
  }
}

