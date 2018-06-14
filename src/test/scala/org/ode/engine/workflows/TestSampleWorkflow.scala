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
 * Author: Alexandre Degurse, Joseph Allemandou
 */

class TestSampleWorkflow
    extends FlatSpec
    with Matchers
    with SharedSparkContext
{

  "SampleWorkflow" should "generate results of expected size" in {

    val spark = SparkSession.builder.getOrCreate

    // Signal processing parameters
    val recordSizeInSec = 0.1f
    val winSize = 100
    val nfft = 100
    val fftOffset = 100
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
      winSize,
      nfft,
      fftOffset
      )

    val resultMap = sampleWorkflow.apply(
      soundUrl,
      soundSamplingRate,
      soundChannels,
      soundSampleSizeInBits)

    val expectedRecordNumber = soundDurationInSecs / recordSizeInSec
    val expectedWindowsPerRecord = soundSamplingRate * recordSizeInSec / winSize
    val expectedFFTSize = nfft + 2 // nfft is even

    resultMap.size should equal(4)
    val ffts = resultMap("ffts").left.get.cache()
    ffts.count should equal(expectedRecordNumber)
    ffts.take(1).map{case (idx, channels) =>
      channels(0).length should equal(expectedWindowsPerRecord)
      channels(0)(0).length should equal(expectedFFTSize)
    }

    val periodograms = resultMap("periodograms").left.get.cache()
    periodograms.count should equal(expectedRecordNumber)
    periodograms.take(1).map{case (idx, channels) =>
      channels(0).length should equal(expectedWindowsPerRecord)
      channels(0)(0).length should equal(expectedFFTSize / 2)
    }

    val welchs = resultMap("welchs").right.get.cache()
    welchs.count should equal(expectedRecordNumber)
    welchs.take(1).map{case (idx, channels) =>
      channels(0).length should equal(expectedFFTSize / 2)
    }

    val spls = resultMap("spls").right.get.cache()
    spls.count should equal(expectedRecordNumber)
    spls.take(1).map{case (idx, channels) =>
      channels(0).length should equal(expectedWindowsPerRecord)
    }

    // TODO -- Explain in readme the epxected result sizes
    // for the various functions. I found them, but more by
    // chance than anything else :)
  }
}
