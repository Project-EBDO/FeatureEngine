package org.ebdo.engine.example

import java.sql.Timestamp
import java.time.{Duration, LocalDateTime}

import org.apache.spark.sql._

/**
  * Timeserie segmentation in Spark
  * Copyright (C) 2017 Project-EBDO
  * Author: Paul Nguyen HD
  *
  * Generates a dataframe containing a statistical value for every
  * windows of the time serie
  *
  *
  * Uses spark sql window API functions to generate timeserie windows.
  * Uses statistical function of the window API to generate statistics on
  * each window
  *
  * @param spark The SparkSession to use to build the resulting dataframe
  * @param timeSerieToSegment the time serie to segment in a dataframe
  */
class TimeserieSegmentation(spark: SparkSession, timeSerieToSegment: DataFrame) {

  /**
    * Function slicing a time serie in smaller time series (windows)
    *
    * @param from the date to begin analysis
    * @param to the date to end analysis
    * @param windowDuration the duration of the wanted windows in sec
    * @param windowStep the step between windows in sec
    * @return the resulting windows in a dataset with two columns: the first one for the beginning of the window,
    *         the second for all the values in the window
    */
  def slicingTimeSerieBetweenTwoDates(from: LocalDateTime, to: LocalDateTime,windowDuration: Int, windowStep: Int): Dataset[WindowsDF] = {
    import spark.implicits._

    // Choose the date to begin and to end analysis
    val timeserieBetweenTwoDates = timeSerieToSegment.select("ts","values")
      .filter(timeSerieToSegment("ts").between(Timestamp.valueOf(from),Timestamp.valueOf(to)))

    // Convert the initial dataframe to a dataset. Loose of performances
    val ds: Dataset[InitialDF] = timeserieBetweenTwoDates.as[InitialDF]

    // Construct windows. First, gather all the values contained in the windows in an array.
    // Second, take the timestamp of the beginning of the window
    val windowValues = ds.map(r => r.values).collect().sliding(windowDuration,windowStep).toArray.map(r => r.flatten)
    val windowTime = ds.map(r=>r.ts).collect().sliding(windowDuration,windowStep).toArray.map(r=>r.head)

    // Return a dataset with two columns: timestamp of the beginning of the window | values of the window
    val dsWindows: Dataset[WindowsDF] = (windowTime,windowValues).zipped.toList.toDF("windowTime","windowValues").as[WindowsDF]
    dsWindows
  }

  /**
    * Function computing some statistical functions on windows
    *
    * @param windowsToProcess The windows to process
    * @param functionToApply The function to apply on each window (here just an example)
    * @return the resulting DF with two columns: the first one for the beginning of the window, the second for
    *         the result of the function for each window
    */
  def statsOnWindows(windowsToProcess: Dataset[WindowsDF], functionToApply: (Array[Double]) => Array[Double]):
                        DataFrame = {
    import spark.implicits._

    // Select the column to apply the function. Here windowValues to apply a function on Array[Double]
    val functionResultOnWindows = windowsToProcess.map(r=> r.windowValues).map(functionToApply)

    // Rebuild a datafram with the new values.
    val dfWindowsUpdated: DataFrame = (windowsToProcess.map(r=> r.windowTime).collect(),functionResultOnWindows.collect())
      .zipped.toList.toDF("windowTime","computations")
    dfWindowsUpdated
  }

  /**
    * Function slicing and computing stats on user-defined windows
    *
    * @param from the date to begin analysis
    * @param to the date to end analysis
    * @param windowDuration the duration of the wanted windows in sec
    * @param windowStep the step between windows in sec
    * @param functionToApply The function to apply on each window
    * @return the resulting windows
    */
  def computeStatsWindows(from: LocalDateTime, to: LocalDateTime, windowDuration: Int,
                          windowStep: Int,functionToApply: (Array[Double]) => Array[Double]): DataFrame={
    // Compute windows
    val windowsToProcess = slicingTimeSerieBetweenTwoDates(from,to,windowDuration,windowStep)

    // Compute stats or another function on windows
    val windowsStats = statsOnWindows(windowsToProcess,functionToApply)
    windowsStats
  }


}
// Case class to build a dataset with the initial dataframe. Assume that the dataframe contains timestamps
// and arrays of doubles
case class InitialDF(ts: Timestamp, values: Array[Double])

// Case class to build a dataset with windows. Assume that the dataset contains timestamps
// and arrays of doubles
case class WindowsDF(windowTime: Timestamp, windowValues: Array[Double])