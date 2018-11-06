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
import org.oceandataexplorer.engine.io.LastRecordAction.{LastRecordAction, Skip}

import org.apache.spark.sql.{SparkSession, DataFrame, Row}
import org.apache.spark.sql.types._

import com.github.nscala_time.time.Imports._
import java.sql.Timestamp

import org.oceandataexplorer.engine.signalprocessing._
import org.oceandataexplorer.engine.signalprocessing.windowfunctions._
import WindowFunctionTypes.{Symmetric, Periodic}

/**
 * Performance test workflow for Spark over Datarmor.
 * Copy from SampleWorkflow before we provied better general Workflow utility.
 *
 * @author Alexandre Degurse, Joseph Allemandou
 *
 * @param spark The SparkSession to use to build resulting RDDs
 * @param recordDurationInSec The duration of a record in the workflow in seconds
 * @param windowSize The size of the segments to be generated
 * @param windowOverlap The generated segments overlap
 * @param nfft The size of the fft-computation window
 * @param numPartitions The number of partitions of the RDD returned by apply method
 * @param lastRecordAction The action to perform when a partial record is encountered
 *
 */
class PerformanceTestWorkflow
(
  val spark: SparkSession,
  val recordDurationInSec: Float,
  val windowSize: Int,
  val windowOverlap: Int,
  val nfft: Int,
  val numPartitions: Option[Int] = None,
  val lastRecordAction: LastRecordAction = Skip
) {

  private val hadoopWavReader = new HadoopWavReader(spark, recordDurationInSec, lastRecordAction)

  private val SingleChannelFeatureType = DataTypes.createArrayType(DoubleType, false)
  private val MultiChannelsFeatureType = DataTypes.createArrayType(SingleChannelFeatureType, false)

  private val schema = StructType(Seq(
    StructField("timestamp", TimestampType, nullable = true),
    StructField("welchs", MultiChannelsFeatureType, nullable = false),
    StructField("spls", MultiChannelsFeatureType, nullable = false)
  ))


  /**
   * Apply method for the workflow
   *
   * @param soundsUri URI-like string pointing to the wav files
   * (Unix globbing is allowed, file:///tmp/{sound0,sound1}.wav is a valid soundsUri)
   * @param soundsNameAndStartDate A list containing all files
   * names and their start date as a DateTime
   * @param soundSamplingRate Sound's samplingRate
   * @param soundChannels Sound's number of channels
   * @param soundSampleSizeInBits The number of bits used to encode a sample
   * @param soundCalibrationFactor The calibration factor for raw sound calibration
   * @return The computed features (SPL and Welch) over the wav files given in soundsUri
   * as a DataFrame of Row(timestamp, spl, welch).
   * The channels are kept inside the tuple value to have multiple dataframe columns
   * instead of a single one with complex content
   */
  def apply(
    soundsUri: String,
    soundsNameAndStartDate: List[(String, DateTime)],
    soundSamplingRate: Float,
    soundChannels: Int,
    soundSampleSizeInBits: Int,
    soundCalibrationFactor: Double = 1.0
  ): DataFrame = {


    val records = hadoopWavReader.readWavRecords(soundsUri,
      soundsNameAndStartDate,
      soundSamplingRate,
      soundChannels,
      soundSampleSizeInBits)

    val soundCalibrationClass = SoundCalibration(soundCalibrationFactor)
    val segmentationClass = Segmentation(windowSize, windowOverlap)
    val fftClass = FFT(nfft, soundSamplingRate)
    val hammingClass = HammingWindowFunction(windowSize, Periodic)
    val hammingNormalizationFactor = hammingClass.densityNormalizationFactor()

    val psdNormalizationFactor = 1.0 / (soundSamplingRate * hammingNormalizationFactor)
    val periodogramClass = Periodogram(nfft, psdNormalizationFactor, soundSamplingRate)

    val welchClass = WelchSpectralDensity(nfft, soundSamplingRate)
    val energyClass = Energy(nfft)

    val results = records
      .mapValues(chans => chans.map(soundCalibrationClass.compute).map(segmentationClass.compute))
      .mapValues(segmentedChans => segmentedChans.map(signalSegment =>
        signalSegment.map(hammingClass.applyToSignal)))
      .mapValues(windowedChans => windowedChans.map(windowedChan =>
        windowedChan.map(fftClass.compute)))
      .mapValues(fftChans =>
        fftChans.map(fftChan => fftChan.map(periodogramClass.compute)))
      .mapValues(periodogramChans =>
        periodogramChans.map(welchClass.compute))
      .map{ case (ts, welchChans) =>
        (ts, welchChans, welchChans.map(welchChan =>
          Array(energyClass.computeSPLFromPSD(welchChan))))
    }

    spark.createDataFrame(results
      .sortBy(t => t._1, true, numPartitions.getOrElse(results.getNumPartitions))
      .map{ case (ts, welch, spls) => Row(new Timestamp(ts), welch, spls)},
      schema
    )
  }
}
