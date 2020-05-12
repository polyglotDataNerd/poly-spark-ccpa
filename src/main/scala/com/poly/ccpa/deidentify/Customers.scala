package com.poly.ccpa.deidentify

import java.util.concurrent._

import com.poly.utils._
import com.poly.ccpa.utility.{Schemas, SparkUtils}
import org.apache.commons.lang3.time.StopWatch
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.storage.StorageLevel


class Customers extends java.io.Serializable {

  val config: ConfigProps = new ConfigProps
  val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
  val date = format.format(new java.util.Date())
  val sw = new StopWatch


  def deIdentify(sparkSession: SparkSession, sc: SparkContext, sql: SQLContext, source: String, target: String, format: String, stringBuilder: java.lang.StringBuilder): Unit = {
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
      val hoststring = "jdbc:mysql://" + utils.getSSMParam("/datacatalog/production/ENDPOINT") + ":3306/" + "datacatalog"
      val uid = utils.getSSMParam("/datacatalog/DATABASE_USER")
      val pw = utils.getSSMParam("/datacatalog/DATABASE_PASSWORD")
      val tableName = "user_audit"

      /*imports functions*/
      /**/ val sourceDF = sql
        .read
        .option("escape", "\"")
        .schema(schemas.gravyCustomers())
        .csv(source)
        .distinct()
        .persist(StorageLevel.MEMORY_ONLY_SER_2)
      sourceDF.createOrReplaceTempView("sourceView")
      //stringBuilder.append("INFO source table count:  " + sourceDF.count()).append("\n")
      println(stringBuilder.toString())

      /*ID service ddb tale*/
      val ddbTableCount = new DDB().getDDBTableCount(config.getPropValues("ddbTableProd"))
      var idService = sql.createDataFrame(sc.emptyRDD[Row], schemas.idService())
      do {
        idService = utils
          .ddbRead(sql, config.getPropValues("ddbTableProd"))
      } while (idService.count() < ddbTableCount)
      idService.persist(StorageLevel.MEMORY_ONLY_SER_2).createOrReplaceTempView("lookupView")

      /**/ val writeDF = sparkSession.sql(
        """
          select distinct
          id,
          '' email,
          created_at,
          updated_at,
          '' first_name,
          '' last_name,
          '' contact_number,
          reference,
          is_rewards_user,
          has_sent_feedback,
          has_opted_in,
          level_up_id,
          '' birthday,
          has_sent_support,
          rewards_started_at,
          settings,
          use_credit,
          tracking_uuid,
          uuid as gid,
          date_format(current_date(), "y-MM-dd hh:mm:ss") dtc
          from sourceView a inner join lookupView b on trim(concat('gravy-',a.id)) = trim(b.sourceSystemId)
          |""".stripMargin)

      val sourceAnon = sparkSession.sql(
        """
          select distinct
          a.*,
          uuid as gid,
          date_format(current_date(), "y-MM-dd hh:mm:ss") dtc
          from sourceView a left join lookupView b on trim(concat('gravy-',a.id)) = trim(b.sourceSystemId)
          |""".stripMargin)
      println(stringBuilder.toString())

      if (format.equals("orc")) {
        val sourceList = source.split("/").toList
        val bucket = sourceList(2)
        val prefix = sourceList(3) + "/" + sourceList(4)

        /*write to ORC DataLake Anonymized*/
        utils.orcWriter(target + "/orc/" + sourceList(4) + "/", writeDF)

        /*write to secured PII s3 Bucket RAW*/
        utils.orcWriter("s3://polyglotDataNerd-bigdata-private/gravy/customers/" + "/orc/" + sourceList(4) + "/", sourceAnon)

        /*remove from STAGING*/
        new S3(bucket, prefix, stringBuilder, "us-west-2").removeOjects()
        new S3(bucket, "customers_testing", stringBuilder, "us-west-2").removeOjects()
        new S3(bucket, "addresses_testing", stringBuilder, "us-west-2").removeOjects()
        new S3(bucket, "addresses", stringBuilder, "us-west-2").removeOjects()
      }

      if (format.equals("gzip")) {
        val sourceList = source.split("/").toList
        val bucket = sourceList(2)
        val prefix = sourceList(3) + "/" + sourceList(4)

        /*write to ORC DataLake Anonymized*/
        utils.gzipWriter(target + "/gzip/" + sourceList(4) + "/", writeDF)

        /*write to secured PII s3 Bucket RAW*/
        utils.gzipWriter("s3://polyglotDataNerd-bigdata-private/gravy/customers/" + "/gzip/" + sourceList(4) + "/", sourceAnon)

        /*remove from STAGING*/
        new S3(bucket, prefix, stringBuilder, "us-west-2").removeOjects()
        new S3(bucket, "customers_testing", stringBuilder, "us-west-2").removeOjects()
        new S3(bucket, "addresses_testing", stringBuilder, "us-west-2").removeOjects()
        new S3(bucket, "addresses", stringBuilder, "us-west-2").removeOjects()
      }

      /*writes to audit table in data catalog RDS for missing users in ID Service*/
      val writeAudit = sparkSession.sql(
        """
          select distinct
            'gravy' as source,
            concat('gravy-',a.id) as source_id,
            email,
            a.created_at,
            date_format(current_date(), "y-MM-dd hh:mm:ss") dtc
          from sourceView a left join lookupView b on trim(concat('gravy-',a.id)) = trim(b.sourceSystemId)
          where b.sourceSystemId is null
          |""".stripMargin)
      if (!writeAudit.count().equals(0)) {
        stringBuilder.append("INFO Missing from ID Service count:  " + utils.intFormatter(writeAudit.count())).append("\n")
        println(stringBuilder.toString())
        new RDSClient(hoststring, uid, pw, "delete from " + tableName + " where source  = 'gravy'").execute()
        utils.dbWrite(hoststring, uid, pw, tableName, writeAudit)
      }

      sw.stop()
      stringBuilder.append("INFO gravy customers app runtime (seconds): " + sw.getTime(TimeUnit.SECONDS)).append("\n")
      println(stringBuilder.toString())

      new EmailWrapper(config.getPropValues("emails"), config.getPropValues("fromemails"),
        "ETL Notification " +
          source.split("/").toList(4) + " SPARK: " + " CCPA anonymizer " + source,
        "File Location Target: " + target + format + "/" + source.split("/").toList(4)
          + "\n" + stringBuilder.toString()).sendEMail()
    }
    catch {
      case e: Exception => {
        println("Exception", e)
        stringBuilder.append("ERROR " + e.getMessage).append("\n")
      }
    }
  }
}

