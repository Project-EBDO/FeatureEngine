/** Copyright (C) 2017-2018 Project-ODE
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.ode.engine.workflows

import java.net.URI
import scala.io.Source
import scala.collection.mutable.WrappedArray

import org.ode.hadoop.io.{TwoDDoubleArrayWritable, WavPcmInputFormat}
import org.apache.hadoop.conf.Configuration

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import com.github.nscala_time.time.Imports._
import org.joda.time.Days

import org.scalatest.{Matchers, BeforeAndAfterEach, FlatSpec}
import org.ode.utils.test.ErrorMetrics
import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}

/**
 * Tests for PerformanceTestWorkflow that compares its computations with ScalaPerformanceTestWorkflow
 *
 * @author Alexandre Degurse, Joseph Allemandou
 */

class TestPerformanceTestWorkflow
    extends FlatSpec
    with Matchers
    with SharedSparkContext
{

  val maxRMSE = 1.0E-16

  "PerformanceTestWorkflow" should "generate results of expected size" in {

    val spark = SparkSession.builder.getOrCreate

    // Signal processing parameters
    val recordSizeInSec = 1.0f
    val soundSamplingRate = 16000.0f
    val segmentSize = 16000
    val segmentOffset = 16000
    val nfft = 16000

    // Sound parameters
    val soundUri = getClass.getResource("/wav/sin_16kHz_2.5s.wav").toURI
    val soundsNameAndStartDate = List(("sin_16kHz_2.5s.wav", new DateTime("1978-04-11T13:14:20.200Z")))
    val soundChannels = 1
    val soundSampleSizeInBits = 16
    val soundDurationInSecs = 2.5f


    val perfTestWorkflow = new PerformanceTestWorkflow(
      spark,
      recordSizeInSec,
      segmentSize,
      segmentOffset,
      nfft
    )

    val results = perfTestWorkflow.apply(
      soundUri.toString,
      soundsNameAndStartDate,
      soundSamplingRate,
      soundChannels,
      soundSampleSizeInBits
    )

    val expectedRecordNumber = (soundDurationInSecs / recordSizeInSec).toInt
    val expectedWindowsPerRecord = soundSamplingRate * recordSizeInSec / segmentSize
    val expectedFFTSize = nfft + 2 // nfft is even


    val sparkWelchs = results.select("welchs").collect()
    val sparkSPL = results.select("spls").collect()

    sparkWelchs.size should equal(expectedRecordNumber)
    sparkSPL.size should equal(expectedRecordNumber)

    sparkWelchs.map{channels =>
      channels.size should equal(1)
      val chans = channels.getSeq(0).asInstanceOf[Seq[Seq[Double]]]
      chans.map(channel => channel.length should equal(expectedFFTSize / 2))
    }

    sparkSPL.map{channels =>
      channels.size should equal(1)
      val chans = channels.getSeq(0).asInstanceOf[Seq[Seq[Double]]]
      chans.map(channel => channel.length should equal(1))
    }

  }

  it should "generate the same results as the pure scala workflow" in {
    val spark = SparkSession.builder.getOrCreate

    // Signal processing parameters
    val recordSizeInSec = 1.0f
    val soundSamplingRate = 16000.0f
    val segmentSize = 16000
    val segmentOffset = 16000
    val nfft = 16000
    val lowFreq = Some(3000.0)
    val highFreq = Some(7000.0)

    // Sound parameters
    val soundUri = getClass.getResource("/wav/sin_16kHz_2.5s.wav").toURI
    val soundChannels = 1
    val soundSampleSizeInBits = 16
    val soundDurationInSecs= 2.5f
    val soundStartDate = "1978-04-11T13:14:20.200Z"
    val soundsNameAndStartDate = List(("sin_16kHz_2.5s.wav", new DateTime(soundStartDate)))


    val perfTestWorkflow = new PerformanceTestWorkflow(
      spark,
      recordSizeInSec,
      segmentSize,
      segmentOffset,
      nfft
    )

    val sparkResults = perfTestWorkflow.apply(
      soundUri.toString,
      soundsNameAndStartDate,
      soundSamplingRate,
      soundChannels,
      soundSampleSizeInBits
    )

    val sparkTs: Array[Long] = sparkResults
      .select("timestamp")
      .collect()
      .map{channels =>
        val javaTs = channels.getTimestamp(0)
        new DateTime(javaTs).instant.millis
      }
      .toArray

    val sparkWelchs: Array[Array[Array[Double]]] = sparkResults
      .select("welchs")
      .collect()
      .map{channels =>
        channels.getSeq(0).asInstanceOf[Seq[Seq[Double]]]
          .map(_.toArray).toArray
      }
      .toArray

    val sparkSPLs: Array[Array[Array[Double]]] = sparkResults
      .select("spls")
      .collect()
      .map{channels =>
        channels.getSeq(0).asInstanceOf[Seq[Seq[Double]]]
          .map(_.toArray).toArray
      }
      .toArray

    val welchs = sparkTs.zip(sparkWelchs)
    val spls = sparkTs.zip(sparkSPLs)

    val scalaWorkflow = new ScalaSampleWorkflow(
      recordSizeInSec,
      segmentSize,
      segmentOffset,
      nfft,
      lowFreq,
      highFreq
    )

    val resultsScala = scalaWorkflow.apply(
      soundUri,
      soundSamplingRate,
      soundChannels,
      soundSampleSizeInBits,
      soundStartDate
    )

    val scalaWelchs = resultsScala("welchs").right.get
    val scalaSPLs = resultsScala("spls").right.get

    ErrorMetrics.rmse(scalaWelchs, welchs) should be < maxRMSE
    ErrorMetrics.rmse(scalaSPLs, spls) should be < maxRMSE
  }

  it should "generate the results with the right timestamps" in {
    val spark = SparkSession.builder.getOrCreate

    // Signal processing parameters
    val recordSizeInSec = 1.0f
    val soundSamplingRate = 16000.0f
    val segmentSize = 16000
    val segmentOffset = 16000
    val nfft = 16000

    // Sound parameters
    val soundUri = getClass.getResource("/wav/sin_16kHz_2.5s.wav").toURI
    val soundChannels = 1
    val soundSampleSizeInBits = 16

    // Usefull for testing
    val soundDurationInSecs= 2.5f
    val soundStartDate = "1978-04-11T13:14:20.200Z"
    val soundsNameAndStartDate = List(("sin_16kHz_2.5s.wav", new DateTime(soundStartDate)))


    val perfTestWorkflow = new PerformanceTestWorkflow(
      spark,
      recordSizeInSec,
      segmentSize,
      segmentOffset,
      nfft
    )

    val results = perfTestWorkflow.apply(
      soundUri.toString,
      soundsNameAndStartDate,
      soundSamplingRate,
      soundChannels,
      soundSampleSizeInBits
    )

    val timestampsSpark = results.select("timestamp").collect()

    val lastRecordStartTime = timestampsSpark.toSeq.last.getTimestamp(0)
    val lastRecordStartDate = new DateTime(lastRecordStartTime)

    val startDate = new DateTime(soundStartDate)

    val duration = lastRecordStartDate.instant.millis - startDate.instant.millis

    duration should equal(1000)

    val expectedLastRecordDate = new DateTime("1978-04-11T13:14:21.200Z")

    lastRecordStartDate should equal(expectedLastRecordDate)
  }

  it should "raise an IllegalArgumentException when a unexpected wav file is encountered" in {
    val spark = SparkSession.builder.getOrCreate

    val soundUri = getClass.getResource("/wav/sin_16kHz_2.5s.wav").toURI
    val soundChannels = 1
    val soundSampleSizeInBits = 16

    val soundStartDate = "1978-04-11T13:14:20.200Z"
    val soundsNameAndStartDate = List(("wrongFileName.wav", new DateTime(soundStartDate)))


    val perfTestWorkflow = new PerformanceTestWorkflow(spark, 0.1f, 100, 100, 100)

    an[IllegalArgumentException] should be thrownBy perfTestWorkflow.apply(soundUri.toString, soundsNameAndStartDate, 1.0f, 1, 16)
  }
}
