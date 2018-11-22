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


import java.sql.Timestamp

import com.github.nscala_time.time.Imports._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.oceandataexplorer.engine.io.HadoopWavReader
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import org.oceandataexplorer.engine.io.LastRecordAction._

import org.oceandataexplorer.engine.signalprocessing._
import org.oceandataexplorer.engine.signalprocessing.windowfunctions._
import org.oceandataexplorer.engine.signalprocessing.windowfunctions.WindowFunctionTypes._


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
 * @param windowSize The size of the segments to be generated
 * @param windowOverlap The generated segments overlap
 * @param nfft The size of the fft-computation window
 * @param lowFreqTOL The lower boundary of the frequency range to study for TOL computation
 * @param highFreqTOL The upper boundary of the frequency range to study for TOL computation
 * @param lastRecordAction The action to perform when a partial record is encountered
 *
 */
class SampleWorkflow
(
  val spark: SparkSession,
  val recordDurationInSec: Float,
  val windowSize: Int,
  val windowOverlap: Int,
  val nfft: Int,
  val lowFreqTOL: Option[Double] = None,
  val highFreqTOL: Option[Double] = None,
  val lastRecordAction: LastRecordAction = Skip
) {

  private val hadoopWavReader = new HadoopWavReader(spark, recordDurationInSec, lastRecordAction)

  private val SingleChannelFeatureType = DataTypes.createArrayType(DoubleType, false)
  private val MultiChannelsFeatureType = DataTypes.createArrayType(SingleChannelFeatureType, false)

  private val schemaWelchSpl = StructType(Seq(
    StructField("timestamp", TimestampType, nullable = true),
    StructField("welch", MultiChannelsFeatureType, nullable = false),
    StructField("spl", MultiChannelsFeatureType, nullable = false)
  ))

  private val schemaTol = StructType(Seq(
    StructField("timestamp", TimestampType, nullable = true),
    StructField("tol", MultiChannelsFeatureType, nullable = false)
  ))

  /**
   * Function computing Welch and SPL over calibrated records.
   *
   * @param calibratedRecords The input calibrated sound signal as a RDD[Record]
   * @param soundSamplingRate Sound's samplingRate
   * @return The computed features (SPL and Welch) over the input calibrated
   * sound signal given in calibratedRecords as a DataFrame of Row(timestamp, spl, welch).
   * The channels are kept inside the tuple value to have multiple dataframe columns
   * instead of a single one with complex content.
   */
  def computeWelchAndSpl(
    calibratedRecords: RDD[Record],
    soundSamplingRate: Float
  ): DataFrame = {
    val segmentationClass = Segmentation(windowSize, windowOverlap)
    val fftClass = FFT(nfft, soundSamplingRate)
    val hammingClass = HammingWindowFunction(windowSize, Periodic)
    val hammingNormalizationFactor = hammingClass.densityNormalizationFactor()
    val psdNormFactor = 1.0 / (soundSamplingRate * hammingNormalizationFactor)
    val periodogramClass = Periodogram(nfft, psdNormFactor, soundSamplingRate)

    val welchClass = WelchSpectralDensity(nfft, soundSamplingRate)
    val energyClass = Energy(nfft)

    val welchAndSpl = calibratedRecords
      .mapValues(chans => chans.map(segmentationClass.compute))
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

    spark.createDataFrame(welchAndSpl
      .map{ case (ts, welch, spls) => Row(new Timestamp(ts), welch, spls)},
      schemaWelchSpl
    )
  }

  /**
   * Function computing TOL over calibrated records.
   *
   * @param calibratedRecords The input calibrated sound signal as a RDD[Record]
   * @param soundSamplingRate Sound's samplingRate
   * @return The computed feature (TOL) over the input calibrated
   * sound signal given in calibratedRecords as a DataFrame of Row(timestamp, tol).
   * The channels are kept inside the tuple value to have multiple dataframe columns
   * instead of a single one with complex content.
   */
  def computeTol(
    calibratedRecords: RDD[Record],
    soundSamplingRate: Float
  ): DataFrame = {

    if (recordDurationInSec < 1.0f) {
      throw new IllegalArgumentException(
        s"Incorrect recordDurationInSec ($recordDurationInSec) for TOL computation"
      )
    }

    // ensure that nfft is higher than recordDurationInFrame
    val tolSegmentSize = soundSamplingRate.toInt
    val tolNfft = tolSegmentSize

    /**
     * Second segmentation is not needed here, TOLs are computed over the
     * whole record using Periodogram to avoid unnecessary computation.
     */
    val segmentationClass = Segmentation(tolSegmentSize, 0)
    val hammingClass = HammingWindowFunction(tolSegmentSize, Periodic)
    val hammingNormalizationFactor = hammingClass.densityNormalizationFactor()
    val psdNormFactor = 1.0 / (soundSamplingRate * hammingNormalizationFactor)
    val fftClass = FFT(tolNfft, soundSamplingRate)
    val periodogramClass = Periodogram(tolNfft, psdNormFactor, soundSamplingRate)
    val tolClass = TOL(tolNfft, soundSamplingRate, lowFreqTOL, highFreqTOL)

    val tol = calibratedRecords
      .mapValues(calibratedChans => calibratedChans.map(segmentationClass.compute))
      .mapValues(segmentedChans =>
        segmentedChans.map(segments => segments.map(hammingClass.applyToSignal)))
      .mapValues(windowedChans =>
        windowedChans.map(windowedSegments => windowedSegments.map(fftClass.compute)))
      .mapValues(spectrumChans =>
        spectrumChans.map(spectrumSegments => spectrumSegments.map(periodogramClass.compute)))
      .mapValues(periodogramChans =>
        periodogramChans.map(periodogramSegments => periodogramSegments.map(tolClass.compute)))
      .mapValues(tolChans => tolChans.map(tolSegments => tolSegments.view.transpose.map(_.sum / tolSegments.length)))


    spark.createDataFrame(tol
      map{ case (ts, tols) => Row(new Timestamp(ts), tols) },
      schemaTol
    )
  }

  /**
   * Apply method for the workflow
   *
   * @param soundUri URI-like string pointing to the wav files
   * (Unix globbing is allowed, file:///tmp/{sound0,sound1}.wav is a valid soundUri)
   * @param soundsNameAndStartDate A list containing all files
   * names and their start date as a DateTime
   * @param soundSamplingRate Sound's samplingRate
   * @param soundChannels Sound's number of channels
   * @param soundSampleSizeInBits The number of bits used to encode a sample
   * @param soundCalibrationFactor The calibration factor for raw sound calibration
   * @return The computed features (SPL, Welch and TOL if defined) over the wav
   * files given in soundUri as a DataFrame of Row(timestamp, spl, welch) or
   * Row(timestamp, spl, welch, tol).
   * The channels are kept inside the tuple value to have multiple dataframe columns
   * instead of a single one with complex content
   */
  def apply(
    soundUri: String,
    soundsNameAndStartDate: List[(String, DateTime)],
    soundSamplingRate: Float,
    soundChannels: Int,
    soundSampleSizeInBits: Int,
    soundCalibrationFactor: Double = 0.0
  ): DataFrame = {


    val records = hadoopWavReader.readWavRecords(soundUri,
      soundsNameAndStartDate,
      soundSamplingRate,
      soundChannels,
      soundSampleSizeInBits)

    val soundCalibrationClass = SoundCalibration(soundCalibrationFactor)

    val calibratedRecords = records
      .mapValues(chans => chans.map(soundCalibrationClass.compute))

    val welchAndSplDf = computeWelchAndSpl(calibratedRecords, soundSamplingRate)

    import spark.implicits._

    val result = if (recordDurationInSec >= 1.0f) {
      val tolDf = computeTol(calibratedRecords, soundSamplingRate)

      welchAndSplDf
        .join(tolDf, tolDf("timestamp") === welchAndSplDf("timestamp"))
        .drop(tolDf("timestamp"))
    } else welchAndSplDf

    result
      .sort($"timestamp")
  }
}
