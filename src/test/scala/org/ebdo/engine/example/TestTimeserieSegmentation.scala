package org.ebdo.engine.example

import java.time._

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}

/**
  * Tests for Timeserie segmentation
  * Copyright (C) 2017  Project-EBDO
  * Author: Paul NHD
  */
class TestTimeserieSegmentation
  extends FlatSpec
    with Matchers
    with SharedSparkContext
    with BeforeAndAfterEach {

  // Global variable to clean execution context for every test
  var timeserieGenerator = null.asInstanceOf[TimeserieGenerator]
  var now = null.asInstanceOf[LocalDateTime]
  var timeserieSegmentation = null.asInstanceOf[TimeserieSegmentation]

  override def beforeEach(): Unit = {
    // It will fail if the 'to' date is earlier than the 'from' date with the timeserieGenerator exception
    val from = LocalDateTime.of(2017, Month.OCTOBER, 16, 18, 30,15)
    val to = LocalDateTime.of(2017, Month.OCTOBER, 16, 18, 32,35)
    val tick = Duration.ofSeconds(1)
    val spark = SparkSession.builder.getOrCreate
    timeserieGenerator = new TimeserieGenerator(spark)
    val testDf = timeserieGenerator.makeTimeserie(from,to,tick,valueName = "values")
    timeserieSegmentation = new TimeserieSegmentation(spark, testDf)
  }

  "slicing a timeserie" should "return a dataset when given correct inputs" in {
    val from = LocalDateTime.of(2017, Month.OCTOBER, 16, 18, 30,15)
    val to = LocalDateTime.of(2017, Month.OCTOBER, 16, 18, 32,35)
    val windowDuration = 1
    val windowStep = 1
    val redultDs = timeserieSegmentation.slicingTimeSerieBetweenTwoDates(from,to, windowDuration,windowStep)

    val expectedSchema = StructType(Seq(
      StructField("windowTime", TimestampType, nullable = true),
      StructField("windowValues", ArrayType(DoubleType,false), nullable = true)
    ))

    redultDs.schema shouldEqual expectedSchema

  }

  it should "return a dataframe when given correct inputs" in {
    val avgArray : (Array[Double]) =>Array[Double] = (v1: Array[Double]) => Array.apply(v1.sum / v1.length)
    val from = LocalDateTime.of(2017, Month.OCTOBER, 16, 18, 30,15)
    val to = LocalDateTime.of(2017, Month.OCTOBER, 16, 18, 32,35)
    val windowDuration = 1
    val windowStep = 1
    val windowDs = timeserieSegmentation.slicingTimeSerieBetweenTwoDates(from,to, windowDuration,windowStep)
    val resultDf = timeserieSegmentation.statsOnWindows(windowDs,avgArray)

    val expectedSchema = StructType(Seq(
      StructField("windowTime", TimestampType, nullable = true),
      StructField("computations", ArrayType(DoubleType,false), nullable = true)
    ))

    resultDf.schema shouldEqual expectedSchema

  }

}
