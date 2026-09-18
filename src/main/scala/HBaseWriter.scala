import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.{DataFrame, Row}

object HBaseWriter {

  private val tableName =
    TableName.valueOf("suspicious_transactions")

  private def createConnection(): Connection = {
    val conf = HBaseConfiguration.create()

    conf.addResource(
      new Path("/opt/hbase-2.5.15-hadoop3/conf/hbase-site.xml")
    )

    ConnectionFactory.createConnection(conf)
  }

  private def addColumn(
      put: Put,
      family: String,
      qualifier: String,
      value: String
  ): Unit = {
    if (value != null) {
      put.addColumn(
        Bytes.toBytes(family),
        Bytes.toBytes(qualifier),
        Bytes.toBytes(value)
      )
    }
  }

  private def writeRows(
      df: DataFrame,
      reason: String,
      batchId: Long
  ): Unit = {

    if (df.isEmpty) {
      return
    }

    df.foreachPartition { partition: Iterator[Row] =>

      val connection = createConnection()

      try {
        val table = connection.getTable(tableName)

        partition.foreach { row =>

          val accountId =
            Option(row.getAs[String]("accountId")).getOrElse("UNKNOWN")

          val windowStart =
            Option(row.getAs[Any]("windowStart"))
              .map(_.toString)
              .getOrElse("UNKNOWN")

          val rowKey =
            s"${accountId}_${reason}_${windowStart}_${batchId}"

          val put = new Put(Bytes.toBytes(rowKey))

          addColumn(
            put,
            "transaction",
            "accountId",
            accountId
          )

          addColumn(
            put,
            "transaction",
            "timestamp",
            windowStart
          )

          addColumn(
            put,
            "fraud",
            "reason",
            reason
          )

          addColumn(
            put,
            "fraud",
            "detectedAt",
            java.time.Instant.now().toString
          )

          addColumn(
            put,
            "fraud",
            "batchId",
            batchId.toString
          )

          reason match {

            case "HIGH_VALUE_WITHDRAWAL" =>
              addColumn(
                put,
                "transaction",
                "transactionId",
                Option(row.getAs[String]("transactionId")).getOrElse("")
              )

              addColumn(
                put,
                "transaction",
                "customerId",
                Option(row.getAs[String]("customerId")).getOrElse("")
              )

              addColumn(
                put,
                "transaction",
                "transactionType",
                Option(row.getAs[String]("transactionType")).getOrElse("")
              )

              addColumn(
                put,
                "transaction",
                "amount",
                Option(row.getAs[Any]("amount"))
                  .map(_.toString)
                  .getOrElse("")
              )

              addColumn(
                put,
                "transaction",
                "location",
                Option(row.getAs[String]("location")).getOrElse("")
              )

              addColumn(
                put,
                "transaction",
                "pinStatus",
                Option(row.getAs[String]("pinStatus")).getOrElse("")
              )

            case "MULTIPLE_WITHDRAWALS" =>
              addColumn(
                put,
                "fraud",
                "withdrawalCount",
                Option(row.getAs[Any]("withdrawalCount"))
                  .map(_.toString)
                  .getOrElse("")
              )

              addColumn(
                put,
                "fraud",
                "totalAmount",
                Option(row.getAs[Any]("totalAmount"))
                  .map(_.toString)
                  .getOrElse("")
              )

            case "MULTIPLE_CITIES" =>
              addColumn(
                put,
                "fraud",
                "cityCount",
                Option(row.getAs[Any]("cityCount"))
                  .map(_.toString)
                  .getOrElse("")
              )

              addColumn(
                put,
                "fraud",
                "cities",
                Option(row.getAs[Seq[String]]("cities"))
                  .map(_.mkString(","))
                  .getOrElse("")
              )

            case "REPEATED_FAILED_PIN" =>
              addColumn(
                put,
                "fraud",
                "failedPinAttempts",
                Option(row.getAs[Any]("failedPinAttempts"))
                  .map(_.toString)
                  .getOrElse("")
              )

            case _ =>
          }

          table.put(put)
        }

        table.close()

      } finally {
        connection.close()
      }
    }

    println(
      s"[HBase] Batch $batchId -> $reason written successfully"
    )
  }

  def writeHighValue(
      df: DataFrame,
      batchId: Long
  ): Unit = {

    val prepared = df
      .withColumnRenamed("timestamp", "windowStart")

    writeRows(
      prepared,
      "HIGH_VALUE_WITHDRAWAL",
      batchId
    )
  }

  def writeMultipleWithdrawals(
      df: DataFrame,
      batchId: Long
  ): Unit = {

    val prepared = df
      .select(
        "accountId",
        "window",
        "withdrawalCount",
        "totalAmount"
      )
      .withColumn(
        "windowStart",
        org.apache.spark.sql.functions.col("window.start")
      )

    writeRows(
      prepared,
      "MULTIPLE_WITHDRAWALS",
      batchId
    )
  }

  def writeMultipleCities(
      df: DataFrame,
      batchId: Long
  ): Unit = {

    val prepared = df
      .select(
        "accountId",
        "window",
        "cityCount",
        "cities"
      )
      .withColumn(
        "windowStart",
        org.apache.spark.sql.functions.col("window.start")
      )

    writeRows(
      prepared,
      "MULTIPLE_CITIES",
      batchId
    )
  }

  def writeFailedPin(
      df: DataFrame,
      batchId: Long
  ): Unit = {

    val prepared = df
      .select(
        "accountId",
        "window",
        "failedPinAttempts"
      )
      .withColumn(
        "windowStart",
        org.apache.spark.sql.functions.col("window.start")
      )

    writeRows(
      prepared,
      "REPEATED_FAILED_PIN",
      batchId
    )
  }
}
