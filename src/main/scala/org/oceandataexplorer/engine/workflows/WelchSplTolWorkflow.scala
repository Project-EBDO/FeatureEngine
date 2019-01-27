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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.oceandataexplorer.engine.io.LastRecordAction._
import org.oceandataexplorer.engine.signalprocessing._
import org.oceandataexplorer.engine.signalprocessing.windowfunctions.WindowFunctionTypes._
import org.oceandataexplorer.engine.signalprocessing.windowfunctions._


/**
 * Welch & SPL & TOL signal processing workflow in Spark.
 * Computes Welchs, SPLs and TOLs over a calibrated signal.
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
class WelchSplTolWorkflow
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

  private val SingleChannelFeatureType = DataTypes.createArrayType(DoubleType, false)
  private val MultiChannelsFeatureType = DataTypes.createArrayType(SingleChannelFeatureType, false)

  private val schema = StructType(Seq(
    StructField("timestamp", TimestampType, nullable = true),
    StructField("welch", MultiChannelsFeatureType, nullable = false),
    StructField("spl", MultiChannelsFeatureType, nullable = false),
    StructField("tol", MultiChannelsFeatureType, nullable = false)
  ))

  // scalastyle:off method.length

  /**
   * Apply method for the workflow
   *
   * @param calibratedRecords The input calibrated sound signal as a RDD[Record]
   * @param soundSamplingRate Sound's samplingRate
   * @return The computed features (SPL, Welch and TOL if defined) over the wav
   * files given in soundUri as a DataFrame of Row(timestamp, spl, welch) or
   * Row(timestamp, spl, welch, tol).
   * The channels are kept inside the tuple value to have multiple dataframe columns
   * instead of a single one with complex content
   */
  def apply(
    calibratedRecords: RDD[Record],
    soundSamplingRate: Float
  ): DataFrame = {

    import spark.implicits._

    // classes for Welch & SPL computation
    val segmentationClass = Segmentation(windowSize, windowOverlap)
    val fftClass = FFT(nfft, soundSamplingRate)
    val hammingClass = HammingWindowFunction(windowSize, Periodic)
    val hammingNormalizationFactor = hammingClass.densityNormalizationFactor()
    val psdNormFactor = 1.0 / (soundSamplingRate * hammingNormalizationFactor)
    val periodogramClass = Periodogram(nfft, psdNormFactor, soundSamplingRate)
    val welchClass = WelchSpectralDensity(nfft, soundSamplingRate)
    val energyClass = Energy(nfft)

    // classes for TOL computation
    val windowSizeTol = soundSamplingRate.toInt
    val nfftTol = windowSizeTol
    val segmentationClassTol = Segmentation(windowSizeTol)
    val hammingClassTol = HammingWindowFunction(windowSizeTol, Periodic)
    val hammingNormalizationFactorTol = hammingClassTol.densityNormalizationFactor()
    val psdNormFactorTol = 1.0 / (soundSamplingRate * hammingNormalizationFactorTol)
    val fftClassTol = FFT(nfftTol, soundSamplingRate)
    val periodogramClassTol = Periodogram(nfftTol, psdNormFactorTol, soundSamplingRate)
    val tolClass = TOL(nfftTol, soundSamplingRate, lowFreqTOL, highFreqTOL)

    /**
     * TOLs are computed over the windows of fixed length (1 second)
     * and then averaged to produce one TOL vector per record.
     */
    val welchSplTol = calibratedRecords
      /**
       * Segment the calibrated record in windows of:
       * - `windowSize` with `windowOverlap` for Welch & SPL
       * - `windowSizeTol = samplingRate` with no overlap for TOL
       */
      .mapValues(calibratedChans => (calibratedChans.map(segmentationClass.compute),
        calibratedChans.map(segmentationClassTol.compute)))
      // apply hamming window function
      .mapValues{ case (segmentedChans, segmentedTolChans) =>
        (segmentedChans.map(signalSegment =>
          signalSegment.map(hammingClass.applyToSignal)),
        segmentedTolChans.map(segments => segments.map(hammingClassTol.applyToSignal)))}
      // compute FFTs over the windows
      .mapValues{ case (windowedChans, windowedTolChans) =>
        (windowedChans.map(windowedChan => windowedChan.map(fftClass.compute)),
        windowedTolChans.map(windowedSegments => windowedSegments.map(fftClassTol.compute)))}
      // compute Periodogram over windows
      .mapValues{ case (fftChans, fftTolChans) =>
        (fftChans.map(fftChan => fftChan.map(periodogramClass.compute)),
        fftTolChans.map(spectrumSegments => spectrumSegments.map(periodogramClassTol.compute)))}
      /**
       * - Welch which produces one power spectral density vector per channel
       *   (i.e. it aggregates the window using Welch method)
       * - TOL which produces one TOL vector per window
       */
      .mapValues{ case (periodogramChans, periodogramTolChans) =>
        (periodogramChans.map(welchClass.compute),
        periodogramTolChans.map(periodogramSegments => periodogramSegments.map(tolClass.compute)))}
      /**
       * - compute SPL from Welch
       * - aggregate (average) TOLs per channel to produce a single TOL vector per channel
       */
      .mapValues{ case (welchChans, tolChans) =>
        (welchChans, welchChans.map(welchChan => Array(energyClass.computeSPLFromPSD(welchChan))),
          tolChans.map(tolSegments =>
            tolSegments.view.transpose.map(_.sum / tolSegments.length).toArray))}
      // format results for convertion to DataFrame
      .map{ case (ts, features) => Row(new Timestamp(ts), features._1, features._2, features._3)}

    spark.createDataFrame(welchSplTol, schema).sort($"timestamp")
  }
}
