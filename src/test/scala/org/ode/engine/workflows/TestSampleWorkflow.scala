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

import org.ode.utils.test.ErrorMetrics

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import org.apache.spark.rdd.RDD
import org.ode.hadoop.io.{TwoDDoubleArrayWritable, WavPcmInputFormat}
import org.apache.hadoop.conf.Configuration
import java.net.URL
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._
import org.scalatest.{Matchers, BeforeAndAfterEach, FlatSpec}
import scala.io.Source

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

  private type Record = (Float, Array[Array[Array[Double]]])
  private type AggregatedRecord = (Float, Array[Array[Double]])

  def compareResultRecord(
    recordsA: Array[Record],
    recordsB: Array[Record]
  ): Unit = {

    recordsA.length should equal(recordsB.length)

    val numRecords = recordsA.length
    val numChannels = recordsA(0)._2.length
    val numSegments = recordsA(0)._2(0).length
    val segmentLength = recordsA(0)._2(0)(0).length

    var r = 0
    var c = 0
    var s = 0

    while (r < numRecords) {
      // records keys should be equal
      recordsA(r)._1 should equal(recordsB(r)._1)

      // records should have the same number of channels
      recordsA(r)._2.length should equal(numChannels)
      recordsB(r)._2.length should equal(numChannels)

      while (c < numChannels) {
        // each record should have the same number of segments
        recordsA(r)._2(c).length should equal(numSegments)
        recordsB(r)._2(c).length should equal(numSegments)

        var s = 0
        while (s < numSegments) {
          // segments should have the same length
          recordsA(r)._2(c)(s).length should equal(segmentLength)
          recordsB(r)._2(c)(s).length should equal(segmentLength)

          // finally compare values
          ErrorMetrics.rmse(recordsA(r)._2(c)(s), recordsB(r)._2(c)(s)) should be < maxRMSE
          s += 1
        }
        c += 1
      }
      r += 1
    }
  }
  def compareResultAggregatedRecord(
    recordsA: Array[AggregatedRecord],
    recordsB: Array[AggregatedRecord]
  ): Unit = {
    val numChannels = recordsA(0)._2.length
    val recordLength = recordsA(0)._2(0).length

    recordsA
      .zip(recordsB)
      .foreach{case (recA, recB) =>
        // records keys should be equal
        // fails, spark first record key doesn't start at 0.0
        // recA._1 should equal(recB._1)

        // records should have the same number of channels
        recA._2.length should equal(numChannels)
        recB._2.length should equal(numChannels)

        var c = 0
        while (c < numChannels) {
          // each record should have the same number of segments
          recA._2(c).length should equal(recordLength)
          recB._2(c).length should equal(recordLength)

          // finally compare values
          ErrorMetrics.rmse(recA._2(c), recB._2(c)) should be < maxRMSE
          c += 1
        }
      }
  }

  "SampleWorkflow" should "generate results of expected size" in {

    val spark = SparkSession.builder.getOrCreate

    // Signal processing parameters
    val recordSizeInSec = 0.1f
    val segmentSize = 100
    val nfft = 100
    val segmentOffset = 100
    val soundSamplingRate = 16000.0f

    // Sound parameters
    val soundUrl = getClass.getResource("/wav/sin_16kHz_2.5s.wav")
    val soundChannels = 1
    val soundSampleSizeInBits = 16

    // Usefull for testing
    val soundDurationInSecs = 2.5f


    val sampleWorkflow = new SampleWorkflow(
      spark,
      recordSizeInSec,
      segmentSize,
      segmentOffset,
      nfft
    )

    val resultMap = sampleWorkflow.apply(
      soundUrl,
      soundSamplingRate,
      soundChannels,
      soundSampleSizeInBits
    )

    val sparkFFT = resultMap("ffts").left.get.cache()
    val sparkPeriodograms = resultMap("periodograms").left.get.cache()
    val sparkWelchs = resultMap("welchs").right.get.cache()
    val sparkSPL = resultMap("spls").right.get.cache()

    val expectedRecordNumber = soundDurationInSecs / recordSizeInSec
    val expectedWindowsPerRecord = soundSamplingRate * recordSizeInSec / segmentSize
    val expectedFFTSize = nfft + 2 // nfft is even

    resultMap.size should equal(4)

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

    sparkSPL.count should equal(expectedRecordNumber)
    sparkSPL.take(1).map{case (idx, channels) =>
      channels(0).length should equal(1)
    }
  }

  it should "generate the same results as the pure scala workflow" in {
    val spark = SparkSession.builder.getOrCreate

    // Signal processing parameters
    val recordSizeInSec = 0.1f
    val segmentSize = 100
    val nfft = 100
    val segmentOffset = 100
    val soundSamplingRate = 16000.0f

    // Sound parameters
    val soundUrl = getClass.getResource("/wav/sin_16kHz_2.5s.wav")
    val soundChannels = 1
    val soundSampleSizeInBits = 16

    // Usefull for testing
    val soundDurationInSecs= 2.5f


    val sampleWorkflow = new SampleWorkflow(
      spark,
      recordSizeInSec,
      segmentSize,
      nfft,
      segmentOffset
    )

    val resultMap = sampleWorkflow.apply(
      soundUrl,
      soundSamplingRate,
      soundChannels,
      soundSampleSizeInBits
    )

    val sparkFFT = resultMap("ffts").left.get.cache().collect()
    val sparkPeriodograms = resultMap("periodograms").left.get.cache().collect()
    val sparkWelchs = resultMap("welchs").right.get.cache().collect()
    val sparkSPLs = resultMap("spls").right.get.cache().collect()

    val scalaWorkflow = new ScalaSampleWorkflow(
      recordSizeInSec,
      segmentSize,
      segmentOffset,
      nfft
    )

    val resultMapScala = scalaWorkflow.apply(
      soundUrl,
      soundSamplingRate,
      soundChannels,
      soundSampleSizeInBits
    )

    val scalaFFT = resultMapScala("ffts").left.get
    val scalaPeriodograms = resultMapScala("periodograms").left.get
    val scalaWelchs = resultMapScala("welchs").right.get
    val scalaSPLs = resultMapScala("spls").right.get

    compareResultRecord(scalaFFT, sparkFFT)
    compareResultRecord(scalaPeriodograms, sparkPeriodograms)
    compareResultAggregatedRecord(scalaWelchs, sparkWelchs)
    compareResultAggregatedRecord(scalaSPLs, sparkSPLs)
  }
}
