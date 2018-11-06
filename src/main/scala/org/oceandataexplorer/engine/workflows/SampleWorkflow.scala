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
import org.oceandataexplorer.engine.io.LastRecordAction._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SparkSession, DataFrame, Row}

import com.github.nscala_time.time.Imports._
import java.sql.Timestamp

import org.oceandataexplorer.engine.signalprocessing._
import org.oceandataexplorer.engine.signalprocessing.windowfunctions._
import WindowFunctionTypes.{Symmetric, Periodic}

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
 * @param lowFreq The low boundary of the frequency range to study for TOL computation
 * @param highFreq The high boundary of the frequency range to study for TOL computation
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
  val lowFreq: Option[Double] = None,
  val highFreq: Option[Double] = None,
  val lastRecordAction: LastRecordAction = Skip
) {

  private val hadoopWavReader = new HadoopWavReader(spark, recordDurationInSec, lastRecordAction)


  /**
   * Function converting a RDD of Aggregated Records to a DataFrame
   *
   * @param aggRDD RDD of AggregatedRecord to be converted
   * @param featureName Name of the feature in the RDD
   * @return The feature of the RDD as a DataFrame
   */
  def aggRecordRDDToDF(
    aggRDD: RDD[AggregatedRecord],
    featureName: String
  ): DataFrame = {

    val SingleChannelFeatureType = DataTypes.createArrayType(DoubleType, false)
    val MultiChannelsFeatureType = DataTypes.createArrayType(SingleChannelFeatureType, false)

    val schema = StructType(Seq(
      StructField("timestamp", TimestampType, nullable = true),
      StructField(featureName, MultiChannelsFeatureType, nullable = false)
    ))

    spark.createDataFrame(
      aggRDD.map{ case (k, v) => Row(new Timestamp(k), v)},
      schema
    )
  }

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
   * @return A map that contains all basic features as RDDs
   */
  def apply(
    soundsUri: String,
    soundsNameAndStartDate: List[(String, DateTime)],
    soundSamplingRate: Float,
    soundChannels: Int,
    soundSampleSizeInBits: Int,
    soundCalibrationFactor: Double = 1.0
  ): Map[String, Either[RDD[SegmentedRecord], RDD[AggregatedRecord]]] = {

    val records = hadoopWavReader.readWavRecords(soundsUri, soundsNameAndStartDate,
      soundSamplingRate, soundChannels, soundSampleSizeInBits
    )

    val soundCalibrationClass = SoundCalibration(soundCalibrationFactor)
    val segmentationClass = Segmentation(windowSize, windowOverlap)
    val fftClass = FFT(nfft, soundSamplingRate)
    val hammingClass = HammingWindowFunction(windowSize, Periodic)
    val hammingNormalizationFactor = hammingClass.densityNormalizationFactor()
    val psdNormalizationFactor = 1.0 / (soundSamplingRate * hammingNormalizationFactor)
    val periodogramClass = Periodogram(nfft, psdNormalizationFactor, soundSamplingRate)
    val welchClass = WelchSpectralDensity(nfft, soundSamplingRate)
    val energyClass = Energy(nfft)

    val ffts = records.mapValues(chans => chans.map(soundCalibrationClass.compute))
      .mapValues(chans => chans.map(segmentationClass.compute))
      .mapValues(segmentedChans => segmentedChans.map(signalSegment =>
        signalSegment.map(hammingClass.applyToSignal)))
      .mapValues(windowedChans => {
        windowedChans.map(windowedChan => windowedChan.map(fftClass.compute))
      })

    val periodograms = ffts.mapValues(fftChans =>
      fftChans.map(fftChan => fftChan.map(periodogramClass.compute)))

    val welchs = periodograms.mapValues(periodogramChans =>
      periodogramChans.map(welchClass.compute))

    val spls = welchs.mapValues(welchChans => welchChans.map(welchChan =>
      Array(energyClass.computeSPLFromPSD(welchChan))))

    val resultMap = Map("ffts" -> Left(ffts), "periodograms" -> Left(periodograms),
      "welchs" -> Right(welchs), "spls" -> Right(spls))

    if (nfft >= soundSamplingRate.toInt) {
      val tolClass = TOL(nfft, soundSamplingRate, lowFreq, highFreq)
      val tols = welchs.mapValues(welchChans => welchChans.map(tolClass.compute))
      resultMap ++ Map("tols" -> Right(tols))
    } else {
      resultMap
    }
  }
}
