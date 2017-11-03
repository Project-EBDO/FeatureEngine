package org.ebdo.engine.example

import java.sql.Timestamp
import java.time.temporal.ChronoUnit
import java.time.{Duration, LocalDateTime}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}



/**
  * Timeserie generator in Spark
  * Copyright (C) 2017 Project-EBDO
  * Author: Joseph Allemandou
  *
  * Generates a dataframe containing a value for every
  * tick in [from, to[ interval.
  * Tick unit is expected to be SECOND at minimum.
  *
  * Uses spark partitions to generate timeserie blocks.
  * Last partition contains remaining ticks when interval
  * modulo tick is not 0.
  * Uses java 8 time to specify boundaries and tick.
  *
  * @param spark The SparkSession to use to build the resulting dataframe
  */
class TimeserieGenerator(spark: SparkSession) extends Serializable {

  /**
    * Utility function to convert a [[java.time.LocalDateTime]] to
    * a [[java.sql.Timestamp]].
    * This function is needed because dataframes use SQL types
    *
    * @param localDateTime the LocalDateTime to convert
    * @return the converted Timestamp
    */
  def dt2ts(localDateTime: LocalDateTime): Timestamp = Timestamp.valueOf(localDateTime)


  /**
    * Function generating a timeserie dataframe.
    *
    * Uses spark partitions as follow:
    *
    * p1: [from ... from + (interval / ticks) - 1)[
    * p2: [from + (interval / ticks) ... from + (interval / ticks) * 2 - 1[
    *  ...
    * pN: [from + (interval / ticks) * (N -1) ... from + (interval / ticks) * N - 1 + interval % ticks [
    *
    * @param from Beginning of timeserie interval generation (inclusive)
    * @param to end of timeserie interval generation (exclusive)
    * @param tick Frequency of data points (minimum 1 second)
    * @param valueBuilder Optional function building a double value from the in-partition index
    *                     Default to identity with toDouble conversion
    * @param numPartitions Optional number of partitions to parallelize genrating the dataframe
    *                      Default to 1
    * @param tsName Optional name of the timestamp field in the dataframe
    *               Default to ts
    * @param valueName Optional name of the value field in the dataframe
    *                  Default to val
    * @return The dataframe[ts, val] of values generated for the time interval at tick frequency
    */
  def makeTimeserie(from: LocalDateTime,
                    to: LocalDateTime,
                    tick: Duration,
                    valueBuilder: Int => Double = (idx: Int) => idx.toDouble,
                    numPartitions: Int = 1,
                    tsName: String = "ts",
                    valueName: String = "values"
                   ): DataFrame ={

    // Needed to easily convert the RDD to a dataframe
    import spark.implicits._

    // Check that tick is smaller than interval duration
    val intervalDuration = Duration.between(from, to)
    if (intervalDuration.minus(tick).isNegative)
      throw new IllegalArgumentException("Tick should be smaller or equal than [from, to[ interval")

    // Precompute tick-based values
    val ticksNumber = intervalDuration.getSeconds / tick.getSeconds
    val ticksPerPartition = (ticksNumber / numPartitions).toInt
    val ticksLeftover = (ticksNumber % numPartitions).toInt
    val regularPartitionDurationSeconds = tick.getSeconds * ticksPerPartition

    // Fake a spark parallel collection (no data) with needed number of partition
    // and apply value generation to each partition (in parallel)
    spark.sparkContext.parallelize(Seq[Int](), numPartitions)
      .mapPartitionsWithIndex { case (partitionIndex, _) =>
        // If current partition is last, pick remaining ticks in addition to regular ones
        val ticksToGenerate: Int = if (partitionIndex < (numPartitions - 1)) ticksPerPartition else ticksPerPartition  + ticksLeftover
        val partitionOffsetSeconds = regularPartitionDurationSeconds * partitionIndex
        (0 to ticksToGenerate - 1).map{ t =>
          val timestamp = dt2ts(from.plus(partitionOffsetSeconds + t, ChronoUnit.SECONDS))
//          (timestamp, (0.0+valueBuilder(t) to 63.0+valueBuilder(t) by 1.0).toArray[Double])
          (timestamp, Array(valueBuilder(t)))
        }.iterator
      }.toDF(tsName, valueName)
  }

  /**
    * Function generating a timeserie dataframe.
    *
    * Uses spark partitions as follow:
    *
    * p1: [from ... from + (interval / ticks) - 1)[
    * p2: [from + (interval / ticks) ... from + (interval / ticks) * 2 - 1[
    *  ...
    * pN: [from + (interval / ticks) * (N -1) ... from + (interval / ticks) * N - 1 + interval % ticks [
    *
    * @param from Beginning of timeserie interval generation (inclusive)
    * @param to end of timeserie interval generation (exclusive)
    * @param tick Frequency of data points (minimum 1 second)
    * @param valueBuilder Optional function building a double value from the in-partition index
    *                     Default to identity with toDouble conversion
    * @param numPartitions Optional number of partitions to parallelize genrating the dataframe
    *                      Default to 1
    * @param tsName Optional name of the timestamp field in the dataframe
    *               Default to ts
    * @return The dataframe[ts, val] of values generated for the time interval at tick frequency
    */
  def makeTimeseriePerso(from: LocalDateTime,
                    to: LocalDateTime,
                    tick: Duration,
                    valueBuilder: Int => Double = (idx: Int) => idx.toDouble,
                    numPartitions: Int = 1,
                    tsName: String = "ts"): Dataset[OnlyTS] ={

    // Needed to easily convert the RDD to a dataframe
    import spark.implicits._

    // Check that tick is smaller than interval duration
    val intervalDuration = Duration.between(from, to)
    if (intervalDuration.minus(tick).isNegative)
      throw new IllegalArgumentException("Tick should be smaller or equal than [from, to[ interval")

    // Precompute tick-based values
    val ticksNumber = intervalDuration.getSeconds / tick.getSeconds
    val ticksPerPartition = (ticksNumber / numPartitions).toInt
    val ticksLeftover = (ticksNumber % numPartitions).toInt
    val regularPartitionDurationSeconds = tick.getSeconds * ticksPerPartition

    // Fake a spark parallel collection (no data) with needed number of partition
    // and apply value generation to each partition (in parallel)
    spark.sparkContext.parallelize(Seq[Int](), numPartitions)
      .mapPartitionsWithIndex { case (partitionIndex, _) =>
        // If current partition is last, pick remaining ticks in addition to regular ones
        val ticksToGenerate: Int = if (partitionIndex < (numPartitions - 1)) ticksPerPartition else ticksPerPartition  + ticksLeftover
        val partitionOffsetSeconds = regularPartitionDurationSeconds * partitionIndex
        (0 to ticksToGenerate - 1).map{ t =>
          val timestamp = dt2ts(from.plus(partitionOffsetSeconds + t, ChronoUnit.SECONDS))
          (timestamp)
        }.iterator
      }.toDF("ts").as[OnlyTS]
  }
}

case class OnlyTS(ts: Timestamp)