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

import java.net.URL

import org.apache.hadoop.io.{DoubleWritable, LongWritable}
import org.ode.hadoop.io.{TwoDDoubleArrayWritable, WavPcmInputFormat}

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

import org.ode.engine.workflows.SparkSignalProcessingWorkflow.{Record, AggregatedRecord}

/**
 * Trait for Spark signal processing workflow.
 * This trait provides some functions used in spark workflow and
 * some abstractions that Spark worflows must implement
 *
 * @author Alexandre Degurse, Joseph Allemandou
 *
 */
trait SparkSignalProcessingWorkflow extends Serializable {
  /**
   * The SparkSession to use to build resulting RDDs
   */
  val spark: SparkSession

  /**
   * The duration of a record in the workflow in seconds
   */
  val recordDurationInSec: Float

  /**
   * The size of the segments to be generated
   */
  val segmentSize: Int

  /**
   * The offset used to segment the signal
   */
  val segmentOffset: Int

  /**
   * The size of the fft-computation window
   */
  val nfft: Int

  /**
   * The action to perform when a partial record is encountered
   */
  val lastRecordAction: String


  /**
   * Apply abstract method for the workflow
   *
   * @param soundUrl The URL to find the sound
   * @param soundSamplingRate Sound's samplingRate
   * @param soundChannels Sound's number of channels
   * @param soundSampleSizeInBits The number of bits used to encode a sample
   * @return A map that contains all computed features as RDDs
   */
  def apply(
    soundUrl: URL,
    soundSamplingRate: Float,
    soundChannels: Int,
    soundSampleSizeInBits: Int
  ): Map[String, Either[RDD[Record], RDD[AggregatedRecord]]]

  /**
   * Function used to read wav files inside a Spark workflow
   *
   * @param soundUrl The URL to find the sound
   * @param soundSamplingRate Sound's samplingRate
   * @param soundChannels Sound's number of channels
   * @param soundSampleSizeInBits The number of bits used to encode a sample
   * @return The records that contains wav's data
   */
  protected def readWavRecords(
    soundUrl: URL,
    soundSamplingRate: Float,
    soundChannels: Int,
    soundSampleSizeInBits: Int
  ): RDD[AggregatedRecord] = {

    val recordSizeInFrame = soundSamplingRate * recordDurationInSec

    if (recordSizeInFrame % 1 != 0.0f) {
      throw new IllegalArgumentException(
        s"Computed record size $recordSizeInFrame should not have a decimal part."
      )
    }

    val hadoopConf = spark.sparkContext.hadoopConfiguration

    WavPcmInputFormat.setSampleRate(hadoopConf, soundSamplingRate)
    WavPcmInputFormat.setChannels(hadoopConf, soundChannels)
    WavPcmInputFormat.setSampleSizeInBits(hadoopConf, soundSampleSizeInBits)
    WavPcmInputFormat.setRecordSizeInFrames(hadoopConf, recordSizeInFrame.toInt)
    WavPcmInputFormat.setPartialLastRecordAction(hadoopConf, lastRecordAction)

    spark.sparkContext.newAPIHadoopFile[LongWritable, TwoDDoubleArrayWritable, WavPcmInputFormat](
      soundUrl.toURI.toString,
      classOf[WavPcmInputFormat],
      classOf[LongWritable],
      classOf[TwoDDoubleArrayWritable],
      hadoopConf
    ).map{ case (writableOffset, writableSignal) =>
      val offsetInSec = writableOffset.get / soundSamplingRate
      val signal = writableSignal.get.map(_.map(_.asInstanceOf[DoubleWritable].get))
      (offsetInSec, signal)
    }
  }
}

/**
 * Spark signal processing trait's object companion.
 * provides the types used in Spark workflows for import
 *
 */

object SparkSignalProcessingWorkflow {
  /**
   * Type for signal processing records.
   * A record represents this:
   * (Float: key/timestamp, Array[Array[Array[Double])
   *                          |     |     |> feature
   *                          |     |> segments
   *                          |> channels
   */
  type Record = (Float, Array[Array[Array[Double]]])
  /**
   * Type for signal processing records.
   * A AggregatedRecord represents this:
   * (Float: key/timestamp, Array[Array[Double])
   *                          |     |> samples/feature
   *                          |> channels
   */
  type AggregatedRecord = (Float, Array[Array[Double]])
}
