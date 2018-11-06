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

package org.oceandataexplorer.engine.io

import LastRecordAction.{Fill, Fail}

import org.apache.spark.sql.SparkSession

import com.github.nscala_time.time.Imports._

import org.apache.spark.SparkException
import org.scalatest.{FlatSpec, Matchers}
import org.oceandataexplorer.utils.test.OdeCustomMatchers
import com.holdenkarau.spark.testing.SharedSparkContext

/**
 * Tests for HadoopWavReader that compares its computations with ScalaSampleWorkflow
 *
 * @author Alexandre Degurse, Joseph Allemandou
 */

class TestHadoopWavReader extends FlatSpec
  with Matchers with SharedSparkContext with OdeCustomMatchers
{

  /**
   * Maximum error allowed for [[OdeCustomMatchers.RmseMatcher]]
   * Test for TOL matches at 6e-12, all other test at 1e-16
   */
  val maxRMSE = 1.0E-11

  it should "read a single wav file when using the readWavRecords and file's name" in {
    val spark = SparkSession.builder.getOrCreate

    val soundUri = getClass.getResource("/wav/sin_16kHz_2.5s.wav").toURI.toString
    val soundChannels = 1
    val soundSampleSizeInBits = 16
    val soundSamplingRate = 16000.0f

    val soundStartDate = "1978-04-11T13:14:20.200Z"
    val soundStartTime = new DateTime(soundStartDate)
    val soundNameAndStartTime = List(("sin_16kHz_2.5s.wav", soundStartTime))

    val hadoopWavReader = new HadoopWavReader(spark, 1.0f)

    val readRecords = hadoopWavReader.readWavRecords(
      soundUri,
      soundNameAndStartTime,
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

  it should "read a single wav file when using the readWavRecords and file's path" in {
    val spark = SparkSession.builder.getOrCreate

    val soundUri = getClass.getResource("/wav/sin_16kHz_2.5s.wav").toURI
    val soundPath = soundUri.getPath
    val soundChannels = 1
    val soundSampleSizeInBits = 16
    val soundSamplingRate = 16000.0f

    val soundStartDate = "1978-04-11T13:14:20.200Z"
    val soundStartTime = new DateTime(soundStartDate)
    val soundNameAndStartTime = List((soundPath, soundStartTime))

    val hadoopWavReader = new HadoopWavReader(spark, 1.0f)

    val readRecords = hadoopWavReader.readWavRecords(
      soundUri.toString,
      soundNameAndStartTime,
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

  it should "read a single wav file when using the readWavRecords wrapper" in {
    val spark = SparkSession.builder.getOrCreate

    val soundUri = getClass.getResource("/wav/sin_16kHz_2.5s.wav").toURI.toString
    val soundChannels = 1
    val soundSampleSizeInBits = 16
    val soundSamplingRate = 16000.0f

    val soundStartDate = "1978-04-11T13:14:20.200Z"
    val soundsStartTime = new DateTime(soundStartDate)

    val hadoopWavReader = new HadoopWavReader(spark, 1.0f)

    val readRecords = hadoopWavReader.readWavRecords(
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

  it should "read a single wav file when filling the incomplete record" in {
    val spark = SparkSession.builder.getOrCreate
    val recordDurationInSec = 1.0f
    val lastRecordAction = Fill

    val soundUri = getClass.getResource("/wav/sin_16kHz_2.5s.wav").toURI
    val soundPath = soundUri.getPath
    val soundChannels = 1
    val soundSampleSizeInBits = 16
    val soundSamplingRate = 16000.0f

    val soundStartDate = "1978-04-11T13:14:20.200Z"
    val soundStartTime = new DateTime(soundStartDate)
    val soundNameAndStartTime = List((soundPath, soundStartTime))

    val hadoopWavReader = new HadoopWavReader(spark, recordDurationInSec, lastRecordAction)

    val readRecords = hadoopWavReader.readWavRecords(
      soundUri.toString,
      soundNameAndStartTime,
      soundSamplingRate,
      soundChannels,
      soundSampleSizeInBits
    ).collect()

    readRecords should have length 3

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

  it should "raise an IOException/SparkException when last record action is fail and an incomplete record is encountered" in {
    val spark = SparkSession.builder.getOrCreate
    val recordDurationInSec = 1.0f
    val lastRecordAction = Fail

    val soundUri = getClass.getResource("/wav/sin_16kHz_2.5s.wav").toURI
    val soundPath = soundUri.getPath
    val soundChannels = 1
    val soundSampleSizeInBits = 16
    val soundSamplingRate = 16000.0f

    val soundStartDate = "1978-04-11T13:14:20.200Z"
    val soundStartTime = new DateTime(soundStartDate)
    val soundNameAndStartTime = List((soundPath, soundStartTime))


    val hadoopWavReader = new HadoopWavReader(spark, recordDurationInSec, lastRecordAction)

    // even though test succeeds, a missive amount of log is displayed
    spark.sparkContext.setLogLevel("OFF")

    val thrown = the[SparkException] thrownBy {
      val readRecords = hadoopWavReader.readWavRecords(
        soundUri.toString,
        soundNameAndStartTime,
        soundSamplingRate,
        soundChannels,
        soundSampleSizeInBits
      ).take(1)
    }

    spark.sparkContext.setLogLevel("WARN")

    thrown.getMessage should include("contains a partial last record and PartialLastRecordAction is set to FAIL.")
  }

  it should "raise an IllegalArgumentException when record size is not round" in {
    val spark = SparkSession.builder.getOrCreate

    val soundUri = getClass.getResource("/wav/sin_16kHz_2.5s.wav").toURI

    val soundStartDate = "1978-04-11T13:14:20.200Z"
    val soundsNameAndStartDate = List(("sin_16kHz_2.5s.wav", new DateTime(soundStartDate)))

    val hadoopWavReader = new HadoopWavReader(spark, 0.1f)

    the[IllegalArgumentException] thrownBy {
      hadoopWavReader.readWavRecords(soundUri.toString, soundsNameAndStartDate, 1.0f, 1, 16)
    } should have message "Computed record size (0.1) should not have a decimal part."
  }

  it should "raise an IOException/SparkException when given a wrong sample rate" in {
    val spark = SparkSession.builder.getOrCreate

    val soundUri = getClass.getResource("/wav/sin_16kHz_2.5s.wav").toURI

    val soundStartDate = "1978-04-11T13:14:20.200Z"
    val soundsNameAndStartDate = List(("sin_12kHz_2.5s.wav", new DateTime(soundStartDate)))


    val hadoopWavReader = new HadoopWavReader(spark, 1.0f)

    // even though test succeeds, a missive amount of log is displayed
    spark.sparkContext.setLogLevel("OFF")

    val thrown = the[SparkException] thrownBy {
      val result = hadoopWavReader.readWavRecords(soundUri.toString, soundsNameAndStartDate, 1.0f, 1, 16)
      result.take(1)
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

    val hadoopWavReader = new HadoopWavReader(spark, 1.0f)

    the[IllegalArgumentException] thrownBy {
      val result = hadoopWavReader.readWavRecords(soundUri.toString, soundsNameAndStartDate, 1.0f, 1, 16)
      result.take(1)
    } should have message "Sounds list contains duplicate filename entries"
  }

  it should "raise an IllegalArgumentException/SparkException when a unexpected wav file is encountered" in {
    val spark = SparkSession.builder.getOrCreate

    val soundUri = getClass.getResource("/wav/sin_16kHz_2.5s.wav").toURI

    val soundStartDate = "1978-04-11T13:14:20.200Z"
    val soundsNameAndStartDate = List(("wrong_name.wav", new DateTime(soundStartDate)))

    val hadoopWavReader = new HadoopWavReader(spark, 1.0f)

    // even though test succeeds, a missive amount of log is displayed
    spark.sparkContext.setLogLevel("OFF")

    val thrown = the[SparkException] thrownBy {
      val result = hadoopWavReader.readWavRecords(soundUri.toString, soundsNameAndStartDate, 16000.0f, 1, 16)
      result.take(1)
    }

    spark.sparkContext.setLogLevel("WARN")

    thrown.getMessage should include("Read file sin_16kHz_2.5s.wav has no startDate in given list")
  }
}
