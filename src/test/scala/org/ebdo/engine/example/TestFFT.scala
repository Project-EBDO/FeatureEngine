package org.ebdo.engine.example

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}



class TestFFT
  extends FlatSpec
    with Matchers
    with SharedSparkContext
    with BeforeAndAfterEach
    with RDDComparisons {

  var test = null.asInstanceOf[FFTTest]

  override def beforeEach(): Unit = {
    val spark = SparkSession.builder.getOrCreate
    test = new FFTTest(spark)
  }

  it should "return an array of length 2 * arrayToProcess  when given arrayToProcess" in {
    val arrayToProcess = (0.0 to 48000.0 by 1.0).toArray
    val FFT = test.FFTArrays(arrayToProcess)

    FFT.length shouldEqual 2*arrayToProcess.length
  }

}
