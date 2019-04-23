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
import org.oceandataexplorer.engine.signalprocessing.windowfunctions._
import org.oceandataexplorer.engine.signalprocessing.windowfunctions.WindowFunctionTypes._


/**
 * TOL signal processing workflow in Spark.
 * Computes Third Octave Levels over a calibrated signal.
 *
 * @author Alexandre Degurse, Joseph Allemandou
 *
 * @param spark The SparkSession to use to build resulting RDDs
 * @param segmentDuration The duration of a segment in the workflow in seconds
 * @param lowFreqTOL The lower boundary of the frequency range to study for TOL computation
 * @param highFreqTOL The upper boundary of the frequency range to study for TOL computation
 * @param lastRecordAction The action to perform when a partial record is encountered
 *
 */


class TolWorkflow
(
  val spark: SparkSession,
  val segmentDuration: Float,
  val lowFreqTOL: Option[Double] = None,
  val highFreqTOL: Option[Double] = None,
  val lastRecordAction: LastRecordAction = Skip
) {

  if (segmentDuration < 1.0f) {
    throw new IllegalArgumentException(
      s"Incorrect segmentDuration ($segmentDuration) for TOL computation"
    )
  }

  private val SingleChannelFeatureType = DataTypes.createArrayType(DoubleType, false)
  private val MultiChannelsFeatureType = DataTypes.createArrayType(SingleChannelFeatureType, false)

  private val schema = StructType(Seq(
    StructField("timestamp", TimestampType, nullable = true),
    StructField("tol", MultiChannelsFeatureType, nullable = false)
  ))

  /**
   * Apply method for the workflow
   *
   * @param calibratedRecords The calibrated sounds records used to compute TOL as a RDD[Record].
   * @param soundSamplingRate Sound's samplingRate
   * @return TOL over the given calibrated sound records as a DataFrame of Row(timestamp, tol).
   * The channels are kept inside the tuple value to have multiple dataframe columns
   * instead of a single one with complex content
   */
  def apply(
    calibratedRecords: RDD[Record],
    soundSamplingRate: Float
  ): DataFrame = {

    import spark.implicits._

    // ensure that nfft is higher than recordDurationInFrame
    val segmentSize = soundSamplingRate.toInt
    val nfft = segmentSize

    /**
     * TOLs are computed over the windows of fixed length (1 second)
     * and then averaged to produce one TOL vector per record.
     */
    val segmentationClass = Segmentation(segmentSize)
    val hammingClass = HammingWindowFunction(segmentSize, Periodic)
    val hammingNormalizationFactor = hammingClass.densityNormalizationFactor()
    val psdNormFactor = 1.0 / (soundSamplingRate * hammingNormalizationFactor)
    val fftClass = FFT(nfft, soundSamplingRate)
    val periodogramClass = Periodogram(nfft, psdNormFactor, soundSamplingRate)
    val tolClass = TOL(nfft, soundSamplingRate, lowFreqTOL, highFreqTOL)

    val tol = calibratedRecords
      .mapValues(calibratedChans => calibratedChans.map(segmentationClass.compute))
      .mapValues(segmentedChans => segmentedChans
        .map(segments => segments.map(hammingClass.applyToSignal)))
      .mapValues(windowedChans => windowedChans
        .map(windowedSegments => windowedSegments.map(fftClass.compute)))
      .mapValues(spectrumChans => spectrumChans
        .map(spectrumSegments => spectrumSegments.map(periodogramClass.compute)))
      .mapValues(periodogramChans => periodogramChans
        .map(periodogramSegments => periodogramSegments.map(tolClass.compute)))
      .mapValues(tolChans => tolChans
        .map(tolSegments => tolSegments.view.transpose
          .map(_.sum / tolSegments.length)))


    spark.createDataFrame(tol
      map{ case (ts, tols) => Row(new Timestamp(ts), tols) },
      schema
    ).sort($"timestamp")
  }
}
