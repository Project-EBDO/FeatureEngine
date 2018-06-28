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
 * Tests for SampleWorkflow that compares its computations with ScalaSampleWorkflow
 *
 * @author Alexandre Degurse, Joseph Allemandou
 */

class TestSampleWorkflow
    extends FlatSpec
    with Matchers
    with SharedSparkContext
{

  val maxRMSE = 1.0E-16

  "SampleWorkflow" should "generate results of expected size" in {

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
    val soundsNameAndStartDate = List(("sin_16kHz_2.5s.wav", new DateTime("1978-04-11T13:14:20.200Z")))
    val soundChannels = 1
    val soundSampleSizeInBits = 16
    val soundDurationInSecs = 2.5f


    val sampleWorkflow = new SampleWorkflow(
      spark,
      recordSizeInSec,
      segmentSize,
      segmentOffset,
      nfft,
      lowFreq,
      highFreq
    )

    val resultMap = sampleWorkflow.apply(
      soundUri,
      soundsNameAndStartDate,
      soundSamplingRate,
      soundChannels,
      soundSampleSizeInBits
    )

    val sparkFFT = resultMap("ffts").left.get.cache()
    val sparkPeriodograms = resultMap("periodograms").left.get.cache()
    val sparkWelchs = resultMap("welchs").right.get.cache()
    val sparkTOLs = resultMap("tols").right.get.cache()
    val sparkSPL = resultMap("spls").right.get.cache()

    val expectedRecordNumber = (soundDurationInSecs / recordSizeInSec).toInt
    val expectedWindowsPerRecord = soundSamplingRate * recordSizeInSec / segmentSize
    val expectedFFTSize = nfft + 2 // nfft is even

    resultMap.size should equal(5)

    sparkFFT.count should equal(expectedRecordNumber)
    sparkFFT.take(1).map{case (idx, channels) =>
      channels(0).length should equal(expectedWindowsPerRecord)
      channels(0)(0).length should equal(expectedFFTSize)
    }

    sparkPeriodograms.count should equal(expectedRecordNumber)
    sparkPeriodograms.take(1).map{case (idx, channels) =>
      channels(0).length should equal(expectedWindowsPerRecord)
      channels(0)(0).length should equal(expectedFFTSize / 2)
    }

    sparkWelchs.count should equal(expectedRecordNumber)
    sparkWelchs.take(1).map{case (idx, channels) =>
      channels(0).length should equal(expectedFFTSize / 2)
    }

    sparkTOLs.count should equal(expectedRecordNumber)
    sparkTOLs.take(1).map{case (idx, channels) =>
      channels(0).length should equal(4)
    }

    sparkSPL.count should equal(expectedRecordNumber)
    sparkSPL.take(1).map{case (idx, channels) =>
      channels(0).length should equal(1)
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


    val sampleWorkflow = new SampleWorkflow(
      spark,
      recordSizeInSec,
      segmentSize,
      segmentOffset,
      nfft,
      lowFreq,
      highFreq
    )

    val resultMap = sampleWorkflow.apply(
      soundUri,
      soundsNameAndStartDate,
      soundSamplingRate,
      soundChannels,
      soundSampleSizeInBits
    )

    val sparkFFT = resultMap("ffts").left.get.cache().collect()
    val sparkPeriodograms = resultMap("periodograms").left.get.cache().collect()
    val sparkWelchs = resultMap("welchs").right.get.cache().collect()
    val sparkTOLs = resultMap("tols").right.get.cache().collect()
    val sparkSPLs = resultMap("spls").right.get.cache().collect()

    val scalaWorkflow = new ScalaSampleWorkflow(
      recordSizeInSec,
      segmentSize,
      segmentOffset,
      nfft,
      lowFreq,
      highFreq
    )

    val resultMapScala = scalaWorkflow.apply(
      soundUri,
      soundSamplingRate,
      soundChannels,
      soundSampleSizeInBits,
      soundStartDate
    )

    val scalaFFT = resultMapScala("ffts").left.get
    val scalaPeriodograms = resultMapScala("periodograms").left.get
    val scalaWelchs = resultMapScala("welchs").right.get
    val scalaTOLs = resultMapScala("tols").right.get
    val scalaSPLs = resultMapScala("spls").right.get

    ErrorMetrics.rmse(scalaFFT, sparkFFT) should be < maxRMSE
    ErrorMetrics.rmse(scalaPeriodograms, sparkPeriodograms) should be < maxRMSE
    ErrorMetrics.rmse(scalaWelchs, sparkWelchs) should be < maxRMSE
    ErrorMetrics.rmse(scalaTOLs, sparkTOLs) should be < maxRMSE
    ErrorMetrics.rmse(scalaSPLs, sparkSPLs) should be < maxRMSE
  }

  it should "generate the results with the right timestamps" in {
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

    // Usefull for testing
    val soundDurationInSecs= 2.5f
    val soundStartDate = "1978-04-11T13:14:20.200Z"
    val soundsNameAndStartDate = List(("sin_16kHz_2.5s.wav", new DateTime(soundStartDate)))


    val sampleWorkflow = new SampleWorkflow(
      spark,
      recordSizeInSec,
      segmentSize,
      segmentOffset,
      nfft,
      lowFreq,
      highFreq
    )

    val resultMap = sampleWorkflow.apply(
      soundUri,
      soundsNameAndStartDate,
      soundSamplingRate,
      soundChannels,
      soundSampleSizeInBits
    )

    val sparkFFT = resultMap("ffts").left.get.cache().collect().sortBy(_._1)

    val lastRecordStartTime = sparkFFT.last._1
    val lastRecordStartDate = new DateTime(lastRecordStartTime)

    val startDate = new DateTime(soundStartDate)

    val duration = lastRecordStartTime - startDate.instant.millis

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


    val sampleWorkflow = new SampleWorkflow(spark, 0.1f, 100, 100, 100)

    an[IllegalArgumentException] should be thrownBy sampleWorkflow.apply(soundUri, soundsNameAndStartDate, 1.0f, 1, 16)
  }
}
