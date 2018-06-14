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
import org.ode.hadoop.io.{TwoDDoubleArrayWritable, WavPcmInputFormat}
import java.net.URL
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.scalatest.{Matchers, BeforeAndAfterEach, FlatSpec}
import scala.io.Source

/**
 * Tests for SampleWorkflow that compares its computations with ScalaSampleWorkflow
 *
 * Author: Alexandre Degurse
 */

class TestSampleWorkflow
    extends FlatSpec
    with Matchers
    with SharedSparkContext
{

  "SampleWorkflow" should "generate the same results than ScalaSampleWorkflow" in {

    val spark = SparkSession.builder.getOrCreate
    val sc = spark.sparkContext
    val hadoopConf = sc.hadoopConfiguration

    val soundUrl = getClass.getResource("/wav/sin_16kHz_2.5s.wav")
    val recordSize = 1000
    val nfft = 100
    val winSize = 100
    val offset = 100
    val soundSamplingRate = 16000.0f
    val soundChannels = 1
    val soundDurationInSecs= 2.5
    val soundSampleSizeInBits = 16
    val slices = 40

    val frameLength = (soundSamplingRate * soundChannels * soundDurationInSecs).toInt

    WavPcmInputFormat.setSampleRate(hadoopConf, soundSamplingRate)
    WavPcmInputFormat.setChannels(hadoopConf, soundChannels)
    WavPcmInputFormat.setSampleSizeInBits(hadoopConf, soundSampleSizeInBits)
    WavPcmInputFormat.setRecordSizeInFrames(hadoopConf, (frameLength / slices).toInt)
    WavPcmInputFormat.setPartialLastRecordAction(hadoopConf, "skip")


    val sampleWorkflow = new SampleWorkflow(
        spark,
        soundUrl,
        recordSize,
        nfft,
        winSize,
        offset,
        soundSamplingRate
        // soundChannels,
        // soundDurationInSecs,
        // soundSampleSizeInBits,
        // slices
      )


    val scalaWorkflow = new ScalaSampleWorkflow(
      soundUrl,
      recordSize,
      nfft,
      winSize,
      offset
    )

    val sparkSpls = sampleWorkflow.spls.collect()
    val scalaSpls = scalaWorkflow.spls
  }
}
