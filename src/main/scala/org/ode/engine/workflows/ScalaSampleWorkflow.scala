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

import java.io.{File, FileInputStream, InputStream}
import java.net.URL
import scala.io.Source

import org.ode.engine.io.WavReader
import org.ode.engine.signal_processing._

/**
 * Class that provides a simple signal processing workflow without using Spark.
 *
 * @author Alexandre Degurse, Joseph Allemandou
 *
 * @param recordDurationInSec The duration of a record in the workflow in seconds
 * @param segmentSize The size of the segments to be generated
 * @param segmentOffset The offset used to segment the signal
 * @param nfft The size of the fft-computation window
 */


class ScalaSampleWorkflow
(
  val recordDurationInSec: Float,
  val segmentSize: Int,
  val segmentOffset: Int,
  val nfft: Int
) {

  private def readRecords(
    soundUrl: URL,
    soundSamplingRate: Float,
    soundChannels: Int,
    soundSampleSizeInBits: Int
  ): Array[AggregatedRecord] = {
    val wavFile: File = new File(soundUrl.toURI)
    val wavReader = new WavReader(wavFile)

    val recordSize = (recordDurationInSec * soundSamplingRate).toInt
    val chunks: Seq[Array[Array[Double]]] = wavReader.readChunks(recordSize)

    chunks.zipWithIndex
      .map{case (record, idx) =>
        (((2*idx * recordSize).toFloat / soundSamplingRate), record)
      }.toArray
  }

  /**
   * Apply method for the workflow
   *
   * @param soundUrl The URL to find the sound
   * @param soundSamplingRate Sound's soundSamplingRate
   * @param soundChannels Sound's number of channels
   * @param soundSampleSizeInBits The number of bits used to encode a sample
   * @return A map that contains all basic features as RDDs
   */
  def apply(
    soundUrl: URL,
    soundSamplingRate: Float,
    soundChannels: Int,
    soundSampleSizeInBits: Int
  ): Map[String, Either[Array[Record], Array[AggregatedRecord]]] = {

    val records = readRecords(soundUrl, soundSamplingRate, soundChannels, soundSampleSizeInBits)

    val segmentationClass = new Segmentation(segmentSize, Some(segmentOffset))
    val hammingClass = new HammingWindow(segmentSize, "symmetric")
    val fftClass = new FFT(nfft)

    val hammingNormalizationFactor = hammingClass.windowCoefficients
      .foldLeft(0.0)((acc, v) => acc + v*v)

    val periodogramClass = new Periodogram(nfft, 1.0/(soundSamplingRate*hammingNormalizationFactor))
    val welchClass = new WelchSpectralDensity(nfft)
    val energyClass = new Energy(nfft)

    val segmented = records.map{
      case (idx, channels) => (idx, channels.map(segmentationClass.compute))
    }

    val ffts = segmented.map{
      case (idx, channels) => (idx, channels.map(_.map(fftClass.compute)))
    }

    val periodograms = ffts.map{
      case (idx, channels) => (idx, channels.map(_.map(periodogramClass.compute)))
    }

    val welchs = periodograms.map{
      case (idx, channels) => (idx, channels.map(welchClass.compute))
    }

    val spls = welchs.map{
      case (idx, channels) => (idx, Array(channels.map(energyClass.computeSPLFromPSD)))
    }

    Map(
      "ffts" -> Left(ffts),
      "periodograms" -> Left(periodograms),
      "welchs" -> Right(welchs),
      "spls" -> Right(spls)
    )
  }
}
