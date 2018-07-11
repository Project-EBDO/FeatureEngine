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

import java.io.{File, FileInputStream, InputStream}
import java.net.URI
import scala.io.Source

import com.github.nscala_time.time.Imports._
import org.joda.time.Days

import org.oceandataexplorer.engine.io.WavReader
import org.oceandataexplorer.engine.signalprocessing._
import org.oceandataexplorer.engine.signalprocessing.windowfunctions._

/**
 * Class that provides a simple signal processing workflow without using Spark.
 *
 * @author Alexandre Degurse, Joseph Allemandou
 *
 * @param recordDurationInSec The duration of a record in the workflow in seconds
 * @param segmentSize The size of the segments to be generated
 * @param segmentOffset The offset used to segment the signal
 * @param nfft The size of the fft-computation window
 * @param lowFreq The low boundary of the frequency range to study for TOL computation
 * @param highFreq The high boundary of the frequency range to study for TOL computation
 */


class ScalaSampleWorkflow
(
  val recordDurationInSec: Float,
  val segmentSize: Int,
  val segmentOffset: Int,
  val nfft: Int,
  val lowFreq: Option[Double] = None,
  val highFreq: Option[Double] = None
) {

  /**
   * Function used to read wav files inside a scala workflow
   *
   * @param soundUri The URI to find the sound
   * @param soundSamplingRate Sound's samplingRate
   * @param soundChannels Sound's number of channels
   * @param soundSampleSizeInBits The number of bits used to encode a sample
   * @param soundStartDate The starting date of the sound file
   * @return The records that contains wav's data
   */
  def readRecords(
    soundUri: URI,
    soundSamplingRate: Float,
    soundChannels: Int,
    soundSampleSizeInBits: Int,
    soundStartDate: String
  ): Array[Record] = {
    val wavFile: File = new File(soundUri)
    val wavReader = new WavReader(wavFile)

    val startTime: Long = new DateTime(soundStartDate).instant.millis

    val recordSize = (recordDurationInSec * soundSamplingRate).toInt
    val chunks: Seq[Array[Array[Double]]] = wavReader.readChunks(recordSize)

    // drop last record if imcomplete
    val completeChunks = if (chunks.head.head.length != chunks.last.last.length) {
      chunks.dropRight(1)
    } else {
      chunks
    }

    completeChunks.zipWithIndex
      .map{case (record, idx) =>
        (startTime + ((1000.0f * idx * recordSize).toFloat / soundSamplingRate).toLong, record)
      }.toArray
  }

  /**
   * Apply method for the workflow
   *
   * @param soundUri The URI to find the sound
   * @param soundSamplingRate Sound's soundSamplingRate
   * @param soundChannels Sound's number of channels
   * @param soundSampleSizeInBits The number of bits used to encode a sample
   * @param soundStartDate The starting date of the sound file
   * @return A map that contains all basic features as RDDs
   */
  def apply(
    soundUri: URI,
    soundSamplingRate: Float,
    soundChannels: Int,
    soundSampleSizeInBits: Int,
    soundStartDate: String = "1970-01-01T00:00:00.000Z"
  ): Map[String, Either[Array[SegmentedRecord], Array[AggregatedRecord]]] = {

    val records = readRecords(
      soundUri,
      soundSamplingRate,
      soundChannels,
      soundSampleSizeInBits,
      soundStartDate)

    val segmentationClass = new Segmentation(segmentSize, Some(segmentOffset))
    val hammingClass = new HammingWindowFunction(segmentSize, "symmetric")
    val hammingNormalizationFactor = hammingClass.densityNormalizationFactor()

    val fftClass = new FFT(nfft, soundSamplingRate)
    val periodogramClass = new Periodogram(
      nfft, 1.0/(soundSamplingRate*hammingNormalizationFactor), 1.0f
    )
    val welchClass = new WelchSpectralDensity(nfft, soundSamplingRate)
    val tolClass = new TOL(nfft, soundSamplingRate, lowFreq, highFreq)
    val energyClass = new Energy(nfft)

    val ffts = records
      .map{case (idx, channels) =>(idx, channels.map(segmentationClass.compute))}
      .map{case (idx, channels) => (idx, channels.map(_.map(hammingClass.applyToSignal)))}
      .map{case (idx, channels) => (idx, channels.map(_.map(fftClass.compute)))}

    val periodograms = ffts.map{
      case (idx, channels) => (idx, channels.map(_.map(periodogramClass.compute)))}

    val welchs = periodograms.map{
      case (idx, channels) => (idx, channels.map(welchClass.compute))}

    val tols = welchs.map{
      case (idx, channels) => (idx, channels.map(tolClass.compute))}

    val spls = welchs.map{
      case (idx, channels) => (idx, Array(channels.map(energyClass.computeSPLFromPSD)))}

    Map(
      "ffts" -> Left(ffts),
      "periodograms" -> Left(periodograms),
      "welchs" -> Right(welchs),
      "tols" -> Right(tols),
      "spls" -> Right(spls))
  }
}
