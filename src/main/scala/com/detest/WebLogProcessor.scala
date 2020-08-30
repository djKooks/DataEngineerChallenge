package com.detest

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.detest.DFProcessor.setupDataframe
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType

object WebLogProcessor {

    def main(args: Array[String]): Unit = {
        println("========================== Start main ==========================")

        val ss = SparkSession
            .builder()
            .appName("WebLogProcessor")
            .master("local[*]")
            .getOrCreate()
        ss.sparkContext.setLogLevel("ERROR")

        val df = setupDataframe(ss)

        println("1. Sessionize the web log by IP = aggregate all page hits by visitor/IP during a session")

        /**
         * Aggregate page hits by session.
         *
         * Session format is
         * - IP: IP address
         * - Session Key: Key value to determine it is same session with previous log with same IP.
         *                It updates when interval with last access is longer than 30 minutes
         *
         */
        val sessionizedDF = df.groupBy(col("session")).count()
            .orderBy(desc("count"))

        exportDataFrame(sessionizedDF, "1_sessionzied")

        println("2. Determine the average session time")

        /**
         * Calculate average session time
         *
         * Calculate duration for all session, and get average value of this.
         *
         */
        val avgSession = df.groupBy("session")
            .agg(
                ((max("timestamp").cast(LongType) - min("timestamp").cast(LongType)) / 1000L).as("duration")
            )
            .agg(
                avg("duration").as("avgDuration")
            )

        exportDataFrame(avgSession, "2_avg")

        println("3. Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session")

        /**
         * Get number of unique url per session
         *
         * Group by session, and count all unique urls
         *
         */
        val uniqueVisits = df
            .groupBy(col("session"))
            .agg(countDistinct(col("url")).as("uniqueUrls"))
            .select(col("session"), col("uniqueUrls"))
            .orderBy(desc("uniqueUrls"))

        exportDataFrame(uniqueVisits, "3_unique-url")

        println("4. Find the most engaged users, ie the IPs with the longest session times")

        /**
         * Sort user with longest session times
         *
         * Group by session, and count all unique urls
         *
         */
        val longestSession = df.groupBy("session")
            .agg(
                ((max("timestamp").cast(LongType) - min("timestamp").cast(LongType)) / 1000L).as("duration")
            )
            .orderBy(desc("duration"))

        exportDataFrame(longestSession, "4_longest-session")

        println("========================== End ==========================")
    }

    def exportDataFrame(df: DataFrame, path: String): Unit =
        df.repartition(1).write
            .format("com.databricks.spark.csv")
            .option("header", "true")
            .csv("result/" + path)

}

