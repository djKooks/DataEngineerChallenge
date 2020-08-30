package com.detest

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType}
import org.apache.spark.sql.{Column, Dataset, SparkSession}
import org.apache.spark.sql.functions.{col, split, lag, when, sum, lit, concat, unix_timestamp}
import org.apache.spark.sql.functions.to_utc_timestamp

object DFProcessor {
    val dataPath = "data/2015_07_22_mktplace_shop_web_log_sample.log.gz"
    val readerOption = Map(
        ("header" -> "false"),
        ("delimiter" -> " "),
        ("quote" -> "\""),
        ("escape" -> "\""),
        ("treatEmptyValuesAsNulls" -> "true")
    )
    val sessionInterval = 1000 * 60 * 30

    def setupDataframe(ss: SparkSession): Dataset[ELBLogFormat] = {
        import ss.implicits._
        val rawDF = ss.read
            .options(readerOption)
            .csv(dataPath)
            .toDF(
                "timestamp",
                "elb",
                "client",
                "backend",
                "requestProcessingTime",
                "backendProcessingTime",
                "responseProcessingTime",
                "elbStatusCode",
                "backendStatusCode",
                "receivedBytes",
                "sendBytes",
                "request",
                "userAgent",
                "sslCipher",
                "sslProtocol"
            )

        val formatted = rawDF
            .where(
                col("timestamp").isNotNull
                && col("client").isNotNull
                && col("request").isNotNull
            )
            .withColumn(
                "timestamp",
                to_utc_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSX")
            )
            .withColumn(
                "elbStatusCode",
                castToInt("elbStatusCode")
            )
            .withColumn(
                "url",
                getUrl
            )
            .withColumn(
                "interval",
                tsInterval
            )
            .withColumn(
                "newSessionFlag",
                when(col("interval") > sessionInterval, true).otherwise(false)
            )
            .withColumn(
                "sessionKey",
                sum(when(col("newSessionFlag"), 1).otherwise(0))
                .over(
                    Window.partitionBy(col("client")).orderBy(col("timestamp"))
                )
            )
            .withColumn(
                "session",
                generateSessionId
            )
            .drop("requestProcessingTime", "backendProcessingTime", "responseProcessingTime", "receivedBytes", "sendBytes")
            .filter(col("elbStatusCode") < 400)


        formatted.as[ELBLogFormat]
    }

    def castToLong(colName: String): Column = col(colName).cast(LongType)
    def castToDouble(colName: String): Column = col(colName).cast(DoubleType)
    def castToInt(colName: String): Column = col(colName).cast(IntegerType)

    /**
     * Get URL from request
     * Remove method and query to identify access from session
     */
    def getUrl(): Column = {
        split(split(col("request"), " ")(1), "\\?")(0)
    }

    /**
     * Calculate timestamp interval with previous request
     */
    def tsInterval(): Column = {
        unix_timestamp(col("timestamp")).minus(
            unix_timestamp(
                lag("timestamp", 1)
                .over(
                    Window.partitionBy(col("client")).orderBy(col("timestamp"))
                )
            )
        )
    }

    /**
     * Generate session ID
     */
    def generateSessionId(): Column = {
        concat(col("client"), lit("__"), sum(when(col("newSessionFlag"), 1).otherwise(0))
            .over(
                Window.partitionBy(col("client")).orderBy(col("timestamp"))
            ))

    }
}
