import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object KafkaStreamingProcessor {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Real-Time ATM Fraud Detection")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    // ==================================================
    // CONFIGURATION
    // ==================================================

    val highAmountThreshold = 50000.0
    val withdrawalThreshold = 3
    val cityThreshold = 3
    val failedPinThreshold = 3

    val transactionSchema = StructType(Array(
      StructField("transactionId", StringType, nullable = false),
      StructField("accountId", StringType, nullable = false),
      StructField("customerId", StringType, nullable = false),
      StructField("transactionType", StringType, nullable = false),
      StructField("amount", DoubleType, nullable = false),
      StructField("timestamp", StringType, nullable = false),
      StructField("location", StringType, nullable = false),
      StructField("pinStatus", StringType, nullable = false)
    ))

    println()
    println("==========================================================")
    println("          REAL-TIME ATM FRAUD DETECTION")
    println("==========================================================")
    println("Kafka          : localhost:9092")
    println("Topic          : bank-transactions")
    println("Window         : 5 minutes")
    println("High Amount    : > ₹50,000")
    println("Withdrawals    : > 3 within window")
    println("Cities         : > 3 within window")
    println("Failed PIN     : > 3 within window")
    println("HBase          : suspicious_transactions")
    println("==========================================================")
    println()

    // ==================================================
    // KAFKA SOURCE
    // ==================================================

    val kafkaRaw = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "bank-transactions")
      .option("startingOffsets", "latest")
      .option("failOnDataLoss", "false")
      .load()

    // ==================================================
    // PARSE JSON
    // ==================================================

    val transactions = kafkaRaw
      .selectExpr("CAST(value AS STRING) AS json")
      .select(from_json($"json", transactionSchema).as("transaction"))
      .select("transaction.*")
      .withColumn(
        "eventTime",
        to_timestamp($"timestamp")
      )
      .filter(
        $"transactionId".isNotNull &&
        $"accountId".isNotNull &&
        $"customerId".isNotNull &&
        $"transactionType".isNotNull &&
        $"amount".isNotNull &&
        $"eventTime".isNotNull &&
        $"location".isNotNull &&
        $"pinStatus".isNotNull
      )
      .withWatermark("eventTime", "2 minutes")

    // ==================================================
    // 1. ALL REAL-TIME TRANSACTIONS
    // ==================================================

    val transactionQuery = transactions
      .select(
        $"transactionId",
        $"accountId",
        $"customerId",
        $"transactionType",
        $"amount",
        $"timestamp",
        $"location",
        $"pinStatus"
      )
      .writeStream
      .format("console")
      .outputMode("append")
      .option("truncate", false)
      .option("numRows", 20)
      .option(
        "checkpointLocation",
        "data/checkpoint/realtime-transactions"
      )
      .start()

    // ==================================================
    // 2. HIGH-VALUE WITHDRAWAL
    // ==================================================

    val highValue = transactions
      .filter(
        upper($"transactionType") === "WITHDRAWAL" &&
        $"amount" > highAmountThreshold
      )
      .select(
        $"transactionId",
        $"accountId",
        $"customerId",
        $"transactionType",
        $"amount",
        $"location",
        $"timestamp",
        $"pinStatus"
      )

    val highValueQuery = highValue
      .writeStream
      .foreachBatch { (batchDF: org.apache.spark.sql.DataFrame, batchId: Long) =>
        batchDF.persist()

        if (!batchDF.isEmpty) {
          println(
            s"[FRAUD] High-value withdrawals detected: ${batchDF.count()}"
          )

          HBaseWriter.writeHighValue(
            batchDF,
            batchId
          )
        }

        batchDF.unpersist()
        ()
      }
      .option(
        "checkpointLocation",
        "data/checkpoint/realtime-high-value"
      )
      .start()

    // ==================================================
    // 3. MULTIPLE WITHDRAWALS
    // ==================================================

    val withdrawalCounts = transactions
      .filter(
        upper($"transactionType") === "WITHDRAWAL"
      )
      .groupBy(
        window($"eventTime", "5 minutes"),
        $"accountId"
      )
      .agg(
        count("*").alias("withdrawalCount"),
        sum($"amount").alias("totalAmount")
      )
      .filter(
        $"withdrawalCount" > withdrawalThreshold
      )

    val withdrawalQuery = withdrawalCounts
      .writeStream
      .foreachBatch { (batchDF: org.apache.spark.sql.DataFrame, batchId: Long) =>

        if (!batchDF.isEmpty) {
          println(
            s"[FRAUD] Multiple withdrawals detected: ${batchDF.count()}"
          )

          HBaseWriter.writeMultipleWithdrawals(
            batchDF,
            batchId
          )
        }
      }
      .outputMode("update")
      .option("checkpointLocation", "data/checkpoint/realtime-withdrawals")
      .start()

    // ==================================================
    // 4. MULTIPLE CITIES
    // ==================================================

    val cityCounts = transactions
      .filter(
        upper($"transactionType") === "WITHDRAWAL"
      )
      .groupBy(
        window($"eventTime", "5 minutes"),
        $"accountId"
      )
      .agg(
        countDistinct($"location").alias("cityCount"),
        collect_set($"location").alias("cities")
      )
      .filter(
        $"cityCount" > cityThreshold
      )

    val cityQuery = cityCounts
      .writeStream
      .foreachBatch { (batchDF: org.apache.spark.sql.DataFrame, batchId: Long) =>

        if (!batchDF.isEmpty) {
          println(
            s"[FRAUD] Multiple cities detected: ${batchDF.count()}"
          )

          HBaseWriter.writeMultipleCities(
            batchDF,
            batchId
          )
        }
      }
      .outputMode("update")
      .option("checkpointLocation", "data/checkpoint/realtime-cities")
      .start()

    // ==================================================
    // 5. REPEATED FAILED PIN
    // ==================================================

    val failedPinCounts = transactions
      .filter(
        upper($"transactionType") === "WITHDRAWAL" &&
        upper($"pinStatus") === "FAILED"
      )
      .groupBy(
        window($"eventTime", "5 minutes"),
        $"accountId"
      )
      .agg(
        count("*").alias("failedPinAttempts")
      )
      .filter(
        $"failedPinAttempts" > failedPinThreshold
      )

    val failedPinQuery = failedPinCounts
      .writeStream
      .foreachBatch { (batchDF: org.apache.spark.sql.DataFrame, batchId: Long) =>

        if (!batchDF.isEmpty) {
          println(
            s"[FRAUD] Repeated failed PIN detected: ${batchDF.count()}"
          )

          HBaseWriter.writeFailedPin(
            batchDF,
            batchId
          )
        }
      }
      .outputMode("update")
      .option("checkpointLocation", "data/checkpoint/realtime-failed-pin")
      .start()

    // ==================================================
    // WAIT
    // ==================================================

    println("==========================================================")
    println(" Spark Structured Streaming is RUNNING")
    println(" Kafka → Spark → Fraud Detection → HBase")
    println(" Waiting for live Kafka transactions...")
    println("==========================================================")

    spark.streams.awaitAnyTermination()
  }
}
