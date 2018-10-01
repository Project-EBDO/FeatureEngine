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

package org.oceandataexplorer.engine.workflows

import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession

import java.sql.Timestamp
import com.github.nscala_time.time.Imports._

import org.apache.spark.SparkException
import org.scalatest.{FlatSpec, Matchers}
import org.oceandataexplorer.utils.test.OdeCustomMatchers
import com.holdenkarau.spark.testing.SharedSparkContext

/**
 * Tests for SampleWorkflow that compares its computations with ScalaSampleWorkflow
 *
 * @author Alexandre Degurse, Joseph Allemandou
 */

class TestSampleWorkflow extends FlatSpec
  with Matchers with SharedSparkContext with OdeCustomMatchers
{

  /**
   * Maximum error allowed for [[OdeCustomMatchers.RmseMatcher]]
   * Test for TOL matches at 6e-12, all other test at 1e-16
   */
  val maxRMSE = 1.0E-11

  "SampleWorkflow" should "generate results of expected size" in {

    val spark = SparkSession.builder.getOrCreate

    // Signal processing parameters
    val recordSizeInSec = 1.0f
    val soundSamplingRate = 16000.0f
    val windowSize = 16000
    val windowOverlap = 0
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
      windowSize,
      windowOverlap,
      nfft,
      lowFreq,
      highFreq
    )

    val resultMap = sampleWorkflow.apply(
      soundUri.toString,
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
    val expectedWindowsPerRecord = soundSamplingRate * recordSizeInSec / windowSize
    val expectedFFTSize = nfft + 2 // nfft is even

    resultMap.size should equal(5)

    sparkFFT.count shouldEqual expectedRecordNumber
    sparkFFT.take(1).foreach{case (idx, channels) =>
      channels(0) should have length expectedWindowsPerRecord.toLong
      channels(0)(0) should have length expectedFFTSize
    }

    sparkPeriodograms.count shouldEqual expectedRecordNumber
    sparkPeriodograms.take(1).foreach{case (idx, channels) =>
      channels(0) should have length expectedWindowsPerRecord.toLong
      channels(0)(0) should have length expectedFFTSize / 2
    }

    sparkWelchs.count should equal(expectedRecordNumber)
    sparkWelchs.take(1).foreach{case (idx, channels) =>
      channels(0) should have length expectedFFTSize / 2
    }

    sparkTOLs.count shouldEqual expectedRecordNumber
    sparkTOLs.take(1).foreach{case (idx, channels) =>
      channels(0) should have length 4
    }

    sparkSPL.count should equal(expectedRecordNumber)
    sparkSPL.take(1).foreach{case (idx, channels) =>
      channels(0) should have length 1
    }
  }

  it should "generate the same results as the pure scala workflow" in {
    val spark = SparkSession.builder.getOrCreate

    // Signal processing parameters
    val recordSizeInSec = 1.0f
    val soundSamplingRate = 16000.0f
    val windowSize = 16000
    val windowOverlap = 0
    val nfft = 16000
    val lowFreq = Some(3000.0)
    val highFreq = Some(7000.0)

    // Sound parameters
    val soundUri = getClass.getResource("/wav/sin_16kHz_2.5s.wav").toURI
    val soundChannels = 1
    val soundSampleSizeInBits = 16
    val soundStartDate = "1978-04-11T13:14:20.200Z"
    val soundsNameAndStartDate = List(("sin_16kHz_2.5s.wav", new DateTime(soundStartDate)))


    val sampleWorkflow = new SampleWorkflow(
      spark,
      recordSizeInSec,
      windowSize,
      windowOverlap,
      nfft,
      lowFreq,
      highFreq
    )

    val resultMap = sampleWorkflow.apply(
      soundUri.toString,
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
      windowSize,
      windowOverlap,
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

    sparkFFT should rmseMatch(scalaFFT)
    sparkPeriodograms should rmseMatch(scalaPeriodograms)
    sparkWelchs should rmseMatch(scalaWelchs)
    sparkSPLs should rmseMatch(scalaSPLs)
    sparkTOLs should rmseMatch(scalaTOLs)
  }

  it should "generate the results with the right timestamps" in {
    val spark = SparkSession.builder.getOrCreate

    // Signal processing parameters
    val recordSizeInSec = 1.0f
    val soundSamplingRate = 16000.0f
    val windowSize = 16000
    val windowOverlap = 0
    val nfft = 16000
    val lowFreq = Some(3000.0)
    val highFreq = Some(7000.0)

    // Sound parameters
    val soundUri = getClass.getResource("/wav/sin_16kHz_2.5s.wav").toURI
    val soundChannels = 1
    val soundSampleSizeInBits = 16

    // Usefull for testing
    val soundStartDate = "1978-04-11T13:14:20.200Z"
    val soundsNameAndStartDate = List(("sin_16kHz_2.5s.wav", new DateTime(soundStartDate)))


    val sampleWorkflow = new SampleWorkflow(
      spark,
      recordSizeInSec,
      windowSize,
      windowOverlap,
      nfft,
      lowFreq,
      highFreq
    )

    val resultMap = sampleWorkflow.apply(
      soundUri.toString,
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
    val expectedLastRecordDate = new DateTime("1978-04-11T13:14:21.200Z")

    duration shouldEqual 1000
    lastRecordStartDate shouldEqual expectedLastRecordDate
  }

  it should "compute tols when given parameters compatible with tol computation" in {
    val spark = SparkSession.builder.getOrCreate

    // Signal processing parameters
    val recordSizeInSec = 1.0f
    val soundSamplingRate = 16000.0f
    val windowSize = 16000
    val windowOverlap = 0
    val nfft = 16000
    val lowFreq = Some(1000.0)
    val highFreq = Some(6000.0)

    // Sound parameters
    val soundUri = getClass.getResource("/wav/sin_16kHz_2.5s.wav").toURI
    val soundChannels = 1
    val soundSampleSizeInBits = 16

    // Usefull for testing
    val soundStartDate = "1978-04-11T13:14:20.200Z"
    val soundsNameAndStartDate = List(("sin_16kHz_2.5s.wav", new DateTime(soundStartDate)))

    val sampleWorkflow = new SampleWorkflow(
      spark,
      recordSizeInSec,
      windowSize,
      windowOverlap,
      nfft,
      lowFreq,
      highFreq
    )

    val resultMap = sampleWorkflow.apply(
      soundUri.toString,
      soundsNameAndStartDate,
      soundSamplingRate,
      soundChannels,
      soundSampleSizeInBits
    )

    val sparkTOL = resultMap("tols").right.get.cache().collect()

    // obtained by applying tol.py from standardization on the first
    // second of the wav file (xin) with:
    /**
     * f, psd = scipy.signal.welch(xin, fs=16000.0, noverlap=0,
     *   window='hamming', nperseg=16000, nfft=16000, detrend=False,
     *   return_onesided=True, scaling='density')
     * tols = tol(psd, 16000.0, 16000, 16000, 1000.0, 6000.0)
     */

    val expectedTOL = Array(
      -3.01037283204366, -111.24321821043127, -109.45049617502617,
      -107.59120472415702, -106.35238654048527, -101.20437081472288,
      -103.64721324107055,  -98.0696920300183 ,  -88.42937775219258
    )

    val tols: Array[Double] = sparkTOL(0)._2(0)

    tols should rmseMatch(expectedTOL)
  }

  it should "read a single wav file" in {
    val spark = SparkSession.builder.getOrCreate

    val soundUri = getClass.getResource("/wav/sin_16kHz_2.5s.wav").toURI.toString
    val soundChannels = 1
    val soundSampleSizeInBits = 16
    val soundSamplingRate = 16000.0f

    val soundStartDate = "1978-04-11T13:14:20.200Z"
    val soundsStartTime = new DateTime(soundStartDate)

    val sampleWorkflow = new SampleWorkflow(spark, 1.0f, 100, 0, 100)
    val readRecords = sampleWorkflow.readWavRecords(
      soundUri,
      soundsStartTime,
      soundSamplingRate,
      soundChannels,
      soundSampleSizeInBits
    ).collect()

    readRecords should have length 2
    readRecords.foreach(segment =>
      segment._2 should have length 1
    )

    val firstWavValues = Array(
      0.00000000000000e+00,  3.82690429687500e-01,
      7.07122802734375e-01,  9.23858642578125e-01,
      9.99969482421875e-01,  9.23889160156250e-01,
      7.07061767578125e-01,  3.82781982421875e-01,
      -9.15527343750000e-05, -3.82629394531250e-01
    )

    readRecords(0)._2(0).take(10) should rmseMatch(firstWavValues)
  }

  it should "create a DataFrame when given a RDD of AggregatedRecord" in {
    val SingleChannelFeatureType = DataTypes.createArrayType(DoubleType, false)
    val MultiChannelsFeatureType = DataTypes.createArrayType(SingleChannelFeatureType, false)

    val expectedSchema = StructType(Seq(
      StructField("timestamp", TimestampType, nullable = true),
      StructField("spls", MultiChannelsFeatureType, nullable = false)
    ))

    val spark = SparkSession.builder.getOrCreate
    val sampleWorkflow = new SampleWorkflow(spark, 1.0f, 100, 0, 100)

    val rdd = spark.sparkContext.parallelize(Seq((10L, Array(Array(1.0)))))
    val df = sampleWorkflow.aggRecordRDDToDF(rdd, "spls")

    df.schema shouldEqual expectedSchema

    df.take(1)(0).getAs[Timestamp](0).toInstant.toEpochMilli shouldEqual 10L
  }

  it should "raise an IllegalArgumentException when record size is not round" in {
    val spark = SparkSession.builder.getOrCreate

    val soundUri = getClass.getResource("/wav/sin_16kHz_2.5s.wav").toURI

    val soundStartDate = "1978-04-11T13:14:20.200Z"
    val soundsNameAndStartDate = List(("sin_16kHz_2.5s.wav", new DateTime(soundStartDate)))

    val sampleWorkflow = new SampleWorkflow(spark, 0.1f, 100, 0, 100)

    the[IllegalArgumentException] thrownBy {
      sampleWorkflow.apply(soundUri.toString, soundsNameAndStartDate, 1.0f, 1, 16)
    } should have message "Computed record size (0.1) should not have a decimal part."
  }

  it should "raise an IOException/SparkException when given a wrong sample rate" in {
    val spark = SparkSession.builder.getOrCreate

    val soundUri = getClass.getResource("/wav/sin_16kHz_2.5s.wav").toURI

    val soundStartDate = "1978-04-11T13:14:20.200Z"
    val soundsNameAndStartDate = List(("wrongFileName.wav", new DateTime(soundStartDate)))


    val sampleWorkflow = new SampleWorkflow(spark, 1.0f, 100, 0, 100)

    // even though test succeeds, a missive amount of log is displayed
    spark.sparkContext.setLogLevel("OFF")

    val thrown = the[SparkException] thrownBy {
      val resultMap = sampleWorkflow.apply(soundUri.toString, soundsNameAndStartDate, 1.0f, 1, 16)
      resultMap("ffts").left.get.cache().take(1)
    }

    spark.sparkContext.setLogLevel("WARN")

    thrown.getMessage should include("sample rate (16000.0) doesn't match configured one (1.0)")
  }

  it should "raise an IllegalArgumentException when given list of files with duplicates" in {
    val spark = SparkSession.builder.getOrCreate

    val soundUri = getClass.getResource("/wav/sin_16kHz_2.5s.wav").toURI

    val soundStartDate = "1978-04-11T13:14:20.200Z"
    val soundsNameAndStartDate = List(
      ("sin_16kHz_2.5s.wav", new DateTime(soundStartDate)),
      ("sin_16kHz_2.5s.wav", new DateTime(soundStartDate))
    )

    val sampleWorkflow = new SampleWorkflow(spark, 1.0f, 100, 0, 100)

    the[IllegalArgumentException] thrownBy {
      val resultMap = sampleWorkflow.apply(soundUri.toString, soundsNameAndStartDate, 1.0f, 1, 16)
      resultMap("ffts").left.get.cache().take(1)
    } should have message "Sounds list contains duplicate filename entries"
  }

  it should "raise an IllegalArgumentException/SparkException when a unexpected wav file is encountered" in {
    val spark = SparkSession.builder.getOrCreate

    val soundUri = getClass.getResource("/wav/sin_16kHz_2.5s.wav").toURI

    val soundStartDate = "1978-04-11T13:14:20.200Z"
    val soundsNameAndStartDate = List(("wrong_name.wav", new DateTime(soundStartDate)))

    val sampleWorkflow = new SampleWorkflow(spark, 1.0f, 100, 0, 100)

    // even though test succeeds, a missive amount of log is displayed
    spark.sparkContext.setLogLevel("OFF")

    val thrown = the[SparkException] thrownBy {
      val resultMap = sampleWorkflow.apply(soundUri.toString, soundsNameAndStartDate, 16000.0f, 1, 16)
      resultMap("ffts").left.get.cache().take(1)
    }

    spark.sparkContext.setLogLevel("WARN")

    thrown.getMessage should include("Read file sin_16kHz_2.5s.wav has no startDate in given list")
  }
}
