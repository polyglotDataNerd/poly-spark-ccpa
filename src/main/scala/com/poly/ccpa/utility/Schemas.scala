package com.poly.ccpa.utility

import org.apache.spark.sql.types._


class Schemas extends Serializable {
  /*need to define MixPanel schemas as structtype arrays as the scala complier gets a java.lang.StackOverflowError because of a
case class limitation on having a max of 22 fields*/

  def gravyCustomers(): StructType = {
    StructType(Seq(
      StructField("id", StringType, true),
      StructField("email", StringType, true),
      StructField("created_at", StringType, true),
      StructField("updated_at", StringType, true),
      StructField("first_name", StringType, true),
      StructField("last_name", StringType, true),
      StructField("contact_number", StringType, true),
      StructField("reference", StringType, true),
      StructField("is_rewards_user", StringType, true),
      StructField("has_sent_feedback", StringType, true),
      StructField("has_opted_in", StringType, true),
      StructField("level_up_id", StringType, true),
      StructField("birthday", StringType, true),
      StructField("has_sent_support", StringType, true),
      StructField("rewards_started_at", StringType, true),
      StructField("settings", StringType, true),
      StructField("use_credit", StringType, true),
      StructField("tracking_uuid", StringType, true)
    )
    )
  }

  def idService(): StructType = {
    StructType(Seq(
      StructField("sourceSystemId", StringType, true),
      StructField("uuid", StringType, true)
    )
    )
  }

  def emaildeletes(): StructType = {
    StructType(Seq(
      StructField("email", StringType, true)
    )
    )
  }

}
