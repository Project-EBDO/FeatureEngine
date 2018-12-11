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

import org.oceandataexplorer.engine.io.HadoopWavReader
import org.oceandataexplorer.engine.signalprocessing.SoundCalibration

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import com.github.nscala_time.time.Imports._

import org.scalatest.{FlatSpec, Matchers}
import com.holdenkarau.spark.testing.SharedSparkContext
import org.oceandataexplorer.utils.test.OdeCustomMatchers


/**
 * Tests for [[TestTolWorkflow]] class
 * It uses [[ScalaSampleWorkflow]] as reference to test [[TestTolWorkflow]] results.
 *
 * @author Alexandre Degurse, Joseph Allemandou
 */

class TestTolWorkflow extends FlatSpec
  with Matchers with SharedSparkContext with OdeCustomMatchers {

  /**
   * Maximum error allowed for [[OdeCustomMatchers.RmseMatcher]]
   */
  val maxRMSE = 1.0E-16

  "TolWorkflow" should "generate results of expected size" in {

    val spark = SparkSession.builder.getOrCreate

    // Signal processing parameters
    val recordSizeInSec = 1.0f
    val soundSamplingRate = 16000.0f
    val nfft = 16000

    // Sound parameters
    val soundUri = getClass.getResource("/wav/sin_16kHz_2.5s.wav").toURI
    val soundsNameAndStartDate = List(("sin_16kHz_2.5s.wav", new DateTime("1978-04-11T13:14:20.200Z")))
    val soundChannels = 1
    val soundSampleSizeInBits = 16
    val soundDurationInSecs = 2.5f

    val hadoopWavReader = new HadoopWavReader(
      spark,
      recordSizeInSec
    )

    val records = hadoopWavReader.readWavRecords(
      soundUri.toString,
      soundsNameAndStartDate,
      soundSamplingRate,
      soundChannels,
      soundSampleSizeInBits
    )

    val calibrationClass = SoundCalibration(0.0)

    val calibratedRecords = records.mapValues(chan => chan.map(calibrationClass.compute))

    val tolWorkflow = new TolWorkflow(
      spark,
      recordSizeInSec
    )

    val results = tolWorkflow(
      calibratedRecords,
      soundSamplingRate
    )

    val expectedRecordNumber = (soundDurationInSecs / recordSizeInSec).toInt
    val expectedNumTob = 39


    val sparkTOL = results.select("tol").collect()

    sparkTOL should have size expectedRecordNumber

    sparkTOL.foreach{channels =>
      channels should have size 1
      val chans = channels.getSeq(0).asInstanceOf[Seq[Seq[Double]]]
      chans.foreach(channel => channel should have length expectedNumTob)
    }
  }

  it should "generate the same results as the pure scala workflow" in {
    val spark = SparkSession.builder.getOrCreate

    // Signal processing parameters
    val recordSizeInSec = 1.0f
    val soundSamplingRate = 16000.0f
    val windowSize = 512
    val windowOverlap = 128
    val nfft = 512
    val lowFreqTOL = Some(3000.0)
    val highFreqTOL = Some(7000.0)

    // Sound parameters
    val soundUri = getClass.getResource("/wav/sin_16kHz_2.5s.wav").toURI
    val soundChannels = 1
    val soundSampleSizeInBits = 16
    val soundStartDate = "1978-04-11T13:14:20.200Z"
    val soundsNameAndStartDate = List(("sin_16kHz_2.5s.wav", new DateTime(soundStartDate)))

    val hadoopWavReader = new HadoopWavReader(
      spark,
      recordSizeInSec
    )

    val records = hadoopWavReader.readWavRecords(
      soundUri.toString,
      soundsNameAndStartDate,
      soundSamplingRate,
      soundChannels,
      soundSampleSizeInBits
    )

    val calibrationClass = SoundCalibration(0.0)

    val calibratedRecords: RDD[Record] = records.mapValues(chan => chan.map(calibrationClass.compute))

    val tolWorkflow = new TolWorkflow(
      spark,
      recordSizeInSec,
      lowFreqTOL,
      highFreqTOL
    )

    val sparkResults = tolWorkflow(
      calibratedRecords,
      soundSamplingRate
    )

    val sparkTs: Array[Long] = sparkResults
      .select("timestamp")
      .collect()
      .map{channels =>
        val javaTs = channels.getTimestamp(0)
        new DateTime(javaTs).instant.millis
      }

    val sparkTOLs: Array[Array[Array[Double]]] = sparkResults
      .select("tol")
      .collect()
      .map{channels =>
        channels.getSeq(0).asInstanceOf[Seq[Seq[Double]]]
          .map(_.toArray).toArray
      }

    val tols = sparkTs.zip(sparkTOLs)

    val scalaWorkflow = new ScalaSampleWorkflow(
      recordSizeInSec,
      windowSize,
      windowOverlap,
      nfft,
      lowFreqTOL,
      highFreqTOL
    )

    val resultsScala = scalaWorkflow(
      soundUri,
      soundSamplingRate,
      soundChannels,
      soundSampleSizeInBits,
      soundStartDate
    )

    val scalaTOLs = resultsScala("tol").right.get

    tols should rmseMatch(scalaTOLs)
  }

  it should "raise an IllegalArgumentException when trying to compute TOL on with recordDuration < 1.0 sec" in {
    val spark = SparkSession.builder.getOrCreate

    // Signal processing parameters
    val recordSizeInSec = 0.1f
    val lowFreqTOL = Some(20.0)
    val highFreqTOL = Some(40.0)

    the[IllegalArgumentException] thrownBy {
       new TolWorkflow(
        spark,
        recordSizeInSec,
        lowFreqTOL,
        highFreqTOL
      )
    } should have message "Incorrect recordDurationInSec (0.1) for TOL computation"
  }
}
