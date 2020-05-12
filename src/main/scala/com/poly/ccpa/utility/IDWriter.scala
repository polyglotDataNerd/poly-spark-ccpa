package com.poly.ccpa.utility

import java.util.concurrent.TimeUnit

import com.poly.utils.{ConfigProps, EmailWrapper}
import org.apache.commons.lang3.time.StopWatch
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.storage.StorageLevel

class IDWriter {

  val config: ConfigProps = new ConfigProps
  val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
  val date = format.format(new java.util.Date())
  val sw = new StopWatch

  def bulkWriter(sparkSession: SparkSession, sc: SparkContext, sql: SQLContext, source: String, stringBuilder: java.lang.StringBuilder): Unit = {
    sw.start()
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
      val sourceDF = sql
        .read
        .option("sep", "	")
        .schema(schemas.idService())
        .csv(source)
        .persist(StorageLevel.MEMORY_ONLY_SER_2)
      sourceDF.createOrReplaceTempView("sourceView")
      stringBuilder.append("INFO source table count:  " + sourceDF.count()).append("\n")
      println(stringBuilder.toString())

      /*ID service ddb tale*/
      val lookup = utils
        .ddbRead(sql, config.getPropValues("ddbTableProd"))
        .persist(StorageLevel.MEMORY_ONLY_SER_2)
      lookup.createOrReplaceTempView("lookupView")

      /*writes to audit table in data catalog RDS for missing users in ID Service*/
      val writeAudit = sparkSession.sql(
        """
          select distinct
            a.sourceSystemId,
            a.uuid
          from sourceView a left join lookupView b on trim(a.sourceSystemId) = trim(b.sourceSystemId)
          where b.sourceSystemId is null
          |""".stripMargin)
        .persist(StorageLevel.MEMORY_ONLY_SER_2)
      stringBuilder.append("INFO Write to DDB count:  " + writeAudit.count()).append("\n")
      println(stringBuilder.toString())
      if (!writeAudit.count().equals(0)) {
        utils.DBB.ddbWrite(writeAudit)
      }

      sw.stop()
      stringBuilder.append("INFO bulk writer app runtime (seconds): " + sw.getTime(TimeUnit.SECONDS)).append("\n")
      println(stringBuilder.toString())

      new EmailWrapper(config.getPropValues("emails"), config.getPropValues("fromemails"),
        "ETL Notification " +
          date + " SPARK: " + " CCPA anonymizer DDB Writer " + config.getPropValues("ddbTableProd"),
        stringBuilder.toString()).sendEMail()
    }
    catch {
      case e: Exception => {
        println("Exception", e)
        stringBuilder.append("ERROR " + e.getMessage).append("\n")
      }
    }
  }

}
