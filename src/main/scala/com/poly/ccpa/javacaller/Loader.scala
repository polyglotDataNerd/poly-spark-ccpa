package com.poly.ccpa.javacaller

import com.poly.anonymizer.Entry
import com.poly.utils._
import org.apache.hadoop.yarn.util.RackResolver
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.SparkSession


/**
  * Created by gbartolome on 11/10/17.
  */
object Loader extends java.io.Serializable {
  val config: ConfigProps = new ConfigProps

  /*set logger*/
  config.loadLog4jprops()
  Logger.getLogger(classOf[RackResolver]).getLevel
  LogManager.getLogger("org").setLevel(Level.DEBUG)
  LogManager.getLogger("akka").setLevel(Level.DEBUG)
  Logger.getLogger("org").setLevel(Level.DEBUG)
  Logger.getLogger("akka").setLevel(Level.DEBUG)

  def main(args: Array[String]): Unit = {

    val sourceBucket: String = args(0)
    val sourceKey: String = args(1)
    val loadFormat: String = args(2)
    val targetBucket: String = args(3)
    val pii: String = args(4)

    val config: ConfigProps = new ConfigProps
    config.loadLog4jprops()
    /*java app will run in local master since it won't parallel because it's not using the Spark framework*/
    val sparkSession = SparkSession
      .builder()
      .appName("spark-ccpa-java-" + loadFormat + "-" + java.util.UUID.randomUUID())
      .config("spark.master", "local[*]")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.rpc.message.maxSize", 2047)
      .config("spark.driver.memory", "100G")
      .config("spark.executor.memory", "100G")
      .config("spark.executor.memoryOverhead", "90G")
      .config("spark.driver.memoryOverhead", "90G")
      .config("spark.driver.maxResultSize", "90G")
      /*increase heap space https://stackoverflow.com/questions/21138751/spark-java-lang-outofmemoryerror-java-heap-space*/
      .config("spark.memory.offHeap.enabled", true)
      .config("spark.memory.offHeap.size", "100G")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryoserializer.buffer.max", 2047)
      .config("org.apache.spark.shuffle.sort.SortShuffleManager", "tungsten-sort")
      .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
      .enableHiveSupport()
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel("ERROR")
    val stringBuilder: java.lang.StringBuilder = new java.lang.StringBuilder

    /*calls java app in com.poly.anonymizer*/
    new Entry().run(sourceBucket, sourceKey, loadFormat, targetBucket, pii, stringBuilder)

    /*deletes original PII directory after anonymization*/
    new S3(sourceBucket, sourceKey, stringBuilder, "us-west-2").removeOjects();

    sparkSession.stop()
    System.exit(0);
  }
}
