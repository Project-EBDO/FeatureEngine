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

import org.ode.engine.signal_processing._

/**
 * Simple signal processing workflow in Spark.
 * This workflow is meant to be a example of how to use all components
 * of this project in a simple use case.
 * We're computing all basic features in this workflow.
 *
 * @author Alexandre Degurse, Joseph Allemandou
 *
 * @param spark The SparkSession to use to build resulting RDDs
 * @param recordDurationInSec The duration of a record in the workflow in seconds
 * @param segmentSize The size of the segments to be generated
 * @param segmentOffset The offset used to segment the signal
 * @param nfft The size of the fft-computation window
 * @param lastRecordAction The action to perform when a partial record is encountered
 *
 */
class SampleWorkflow
(
  val spark: SparkSession,
  val recordDurationInSec: Float,
  val segmentSize: Int,
  val segmentOffset: Int,
  val nfft: Int,
  val lastRecordAction: String = "skip"
) {

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

  /**
   * Apply method for the workflow
   *
   * @param soundUrl The URL to find the sound
   * @param soundSamplingRate Sound's samplingRate
   * @param soundChannels Sound's number of channels
   * @param soundSampleSizeInBits The number of bits used to encode a sample
   * @return A map that contains all basic features as RDDs
   */
  def apply(
    soundUrl: URL,
    soundSamplingRate: Float,
    soundChannels: Int,
    soundSampleSizeInBits: Int
  ): Map[String, Either[RDD[Record], RDD[AggregatedRecord]]] = {

    val records = readWavRecords(soundUrl, soundSamplingRate, soundChannels, soundSampleSizeInBits)

    val segmentationClass = new Segmentation(segmentSize, Some(segmentOffset))
    val fftClass = new FFT(nfft)
    val hammingClass = new HammingWindow(segmentSize, "symmetric")
    val hammingNormalizationFactor = hammingClass.windowCoefficients
      .map(x => x*x).foldLeft(0.0)((acc, v) => acc + v)

    val periodogramClass = new Periodogram(nfft, 1.0/(soundSamplingRate*hammingNormalizationFactor))
    val welchClass = new WelchSpectralDensity(nfft)
    val energyClass = new Energy(nfft)

    val segmented = records.mapValues(channels => channels.map(segmentationClass.compute))

    val ffts = segmented.mapValues(
      channels => channels.map(signalSegment => signalSegment.map(fftClass.compute))
    )

    val periodograms = ffts.mapValues(
      channels => channels.map(fftSegment => fftSegment.map(periodogramClass.compute))
    )
    val welchs = periodograms.mapValues(channels => channels.map(welchClass.compute))

    val spls = welchs.mapValues(welch => Array(welch.map(energyClass.computeSPLFromPSD)))

    Map(
      "ffts" -> Left(ffts),
      "periodograms" -> Left(periodograms),
      "welchs" -> Right(welchs),
      "spls" -> Right(spls)
    )
  }
}
