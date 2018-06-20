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

import org.ode.utils.test.ErrorMetrics.rmse

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
    // make sure that both records have the same dimensions
    recordsA
      .zip(recordsB)
      .foreach{
        recordTuple =>
      }

    val numChannels = recordsA(0)._2.length
    val segmentLength = recordsA(0)._2(0).length

    recordsA
      .zip(recordsB)
      .map(fftTuple => (fftTuple._1._2(0), fftTuple._2._2(0)))
      .foreach{fftTuple =>
        var i = 0
        while (i < fftTuple._1.length) {
          rmse(fftTuple._1(i), fftTuple._2(i)) should be < maxRMSE
          i += 1
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

    val sparkFFT = resultMap("ffts").left.get.cache()
    val sparkPeriodograms = resultMap("periodograms").left.get.cache()
    val sparkWelchs = resultMap("welchs").right.get.cache()
    val sparkSPLs = resultMap("spls").right.get.cache()

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

    sparkFFT
      .collect()
      .zip(scalaFFT)
      .map(fftTuple => (fftTuple._1._2(0), fftTuple._2._2(0)))
      .foreach{fftTuple =>
        var i = 0
        while (i < fftTuple._1.length) {
          rmse(fftTuple._1(i), fftTuple._2(i)) should be < maxRMSE
          i += 1
        }
      }

    sparkPeriodograms
      .collect()
      .zip(scalaPeriodograms)
      .map(periodogramTuple => (periodogramTuple._1._2(0), periodogramTuple._2._2(0)))
      .foreach{periodogramTuple =>
        var i = 0
        while (i < periodogramTuple._1.length) {
          rmse(periodogramTuple._1(i), periodogramTuple._2(i)) should be < maxRMSE
          i += 1
        }
      }


    sparkWelchs
      .collect()
      .zip(scalaWelchs)
      .map(welchTuple => (welchTuple._1._2(0), welchTuple._2._2(0)))
      .foreach(welchTuple =>
        rmse(welchTuple._1, welchTuple._2) should be < maxRMSE
      )

    sparkSPLs
      .collect()
      .zip(scalaSPLs)
      .foreach( splTuple => splTuple._1._2(0) should be(splTuple._2._2(0)))

  }
}
