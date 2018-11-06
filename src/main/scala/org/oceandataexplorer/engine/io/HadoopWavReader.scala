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


import java.net.URI

import LastRecordAction.{Fail, Fill, LastRecordAction, Skip}
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.{NewHadoopRDD, RDD}
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.io.{DoubleWritable, LongWritable}
import org.oceandataexplorer.hadoop.io.{TwoDDoubleArrayWritable, WavPcmInputFormat}
import org.oceandataexplorer.engine.workflows.Record
import com.github.nscala_time.time.Imports._


/**
 * Hadoop-based Wave-file reader class.
 *
 * @param spark The SparkSession to use to build resulting RDDs
 * @param recordDurationInSec The duration of a record in the workflow in seconds
 * @param lastRecordAction The action to perform when a partial record is encountered
 */
class HadoopWavReader
(
  val spark: SparkSession,
  val recordDurationInSec: Float,
  val lastRecordAction: LastRecordAction = Skip
) {

  /**
   * Function used to read wav files inside a Spark workflow
   *
   * @todo pass sounds path instead of names in soundsNameAndStartDate to avoid duplicate when
   * files have the same name but different path
   * @todo read only the files of the list
   *
   * @param soundsUri URI-like string pointing to the wav files
   * (Unix globbing is allowed, file:///tmp/{sound0,sound1}.wav is a valid soundsUri)
   * @param soundsNameAndStartDate A list containing all files names or paths
   * and their start date as a DateTime
   * @param soundSamplingRate Sound's sampling rate
   * @param soundChannels Sound's number of channels
   * @param soundSampleSizeInBits The number of bits used to encode a sample
   * @return The records that contains wav's data
   */
  def readWavRecords(
    soundsUri: String,
    soundsNameAndStartDate: List[(String, DateTime)],
    soundSamplingRate: Float,
    soundChannels: Int,
    soundSampleSizeInBits: Int
  ): RDD[Record] = {

    val recordSizeInFrame = soundSamplingRate * recordDurationInSec

    validateInput(soundsNameAndStartDate, recordSizeInFrame)

    val hadoopConf = setupConf(
      soundSamplingRate, soundChannels, soundSampleSizeInBits, recordSizeInFrame.toInt
    )

    spark.sparkContext.newAPIHadoopFile[LongWritable, TwoDDoubleArrayWritable, WavPcmInputFormat](
      soundsUri,
      classOf[WavPcmInputFormat],
      classOf[LongWritable],
      classOf[TwoDDoubleArrayWritable],
      hadoopConf)
    .asInstanceOf[NewHadoopRDD[LongWritable, TwoDDoubleArrayWritable]]
    .mapPartitionsWithInputSplit{ (inputSplit, iterator) =>
      val filePath: String = inputSplit.asInstanceOf[FileSplit].getPath.toUri.getPath
      val fileName: String = inputSplit.asInstanceOf[FileSplit].getPath.getName

      val startDate = soundsNameAndStartDate
        .find{case (name, _) => name == fileName || name == filePath}.map(_._2)

      if (startDate.isEmpty) {
        throw new IllegalArgumentException(
          s"Read file $fileName has no startDate in given list")}

      iterator.map{ case (writableOffset, writableSignal) =>
        val offsetInMillis = (startDate.get.instant.millis
          + (1000.0f * writableOffset.get.toFloat / soundSamplingRate).toLong)
        val signal = writableSignal.get.map(_.map(_.asInstanceOf[DoubleWritable].get))
        (offsetInMillis, signal)
      }
    }
    .asInstanceOf[RDD[Record]]
  }

  /**
   * Wrapper function used to read a single file
   *
   * @param soundUri URI pointing to a wav file as a String
   * @param soundStartDate The start date of the recording
   * @param soundSamplingRate Sound's samplingRate
   * @param soundChannels Sound's number of channels
   * @param soundSampleSizeInBits The number of bits used to encode a sample
   * @return The records that contains wav's data
   */
  def readWavRecords(
    soundUri: String,
    soundStartDate: DateTime,
    soundSamplingRate: Float,
    soundChannels: Int,
    soundSampleSizeInBits: Int
  ): RDD[Record] = {
    readWavRecords(
      soundUri,
      List((new URI(soundUri).getPath.split("/").last, soundStartDate)),
      soundSamplingRate,
      soundChannels,
      soundSampleSizeInBits
    )
  }

  /**
   * Method configuring the [[WavPcmInputFormat]] through hadoop configuration parameters
   */
  private def setupConf(
    soundSamplingRate: Float,
    soundChannels: Int,
    soundSampleSizeInBits: Int,
    recordSizeInFrame: Int
  ) = {
    val hadoopConf = spark.sparkContext.hadoopConfiguration

    WavPcmInputFormat.setSampleRate(hadoopConf, soundSamplingRate)
    WavPcmInputFormat.setChannels(hadoopConf, soundChannels)
    WavPcmInputFormat.setSampleSizeInBits(hadoopConf, soundSampleSizeInBits)
    WavPcmInputFormat.setRecordSizeInFrames(hadoopConf, recordSizeInFrame)
    WavPcmInputFormat.setPartialLastRecordAction(hadoopConf, lastRecordAction.toString)

    hadoopConf
  }


  /**
   * Method checking on [[readWavRecords]] arguments
   */
  private def validateInput(
    soundsNameAndStartDate: List[(String, DateTime)],
    recordSizeInFrame: Float
  ){
    if (recordSizeInFrame % 1 != 0.0f) {
      throw new IllegalArgumentException(
        s"Computed record size ($recordSizeInFrame) should not have a decimal part."
      )
    }

    if (soundsNameAndStartDate.lengthCompare(soundsNameAndStartDate.distinct.length) != 0) {
      throw new IllegalArgumentException(
        "Sounds list contains duplicate filename entries"
      )
    }
  }
}
