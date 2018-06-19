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

import org.ode.engine.signal_processing._
import org.ode.engine.io.WavReader

/**
 * Class that provides a simple signal processing workflow without using Spark.
 *
 *
 * @author Alexandre Degurse
 */

// scalastyle:off

class ScalaSampleWorkflow (
  val nfft: Int,
  val winSize: Int,
  val offset: Int
){

  private type Record = (Float, Array[Array[Array[Double]]])
  private type AggregatedRecord = (Float, Array[Array[Double]])

  val segmentationClass = new Segmentation(winSize, Some(offset))
  val fftClass = new FFT(nfft)
  val hammingClass = new HammingWindow(winSize, "symmetric")

  val hammingNormalizationFactor = hammingClass.windowCoefficients
    .foldLeft(0.0)((acc, v) => acc + v*v)

  val welchClass = new WelchSpectralDensity(nfft)
  val energyClass = new Energy(nfft)

  def apply(
    soundFilePath: URL,
    recordSize: Int,
    samplingRate: Float
  ): Map[String, Either[Array[Record], Array[AggregatedRecord]]] = {

    val wavFile: File = new File(soundFilePath.toURI)
    val wavReader = new WavReader(wavFile)
    val periodogramClass = new Periodogram(nfft, 1.0 / (samplingRate * hammingNormalizationFactor))

    val chunks: Seq[Array[Array[Double]]] = wavReader.readChunks(recordSize)
    val records: Array[(Float, Array[Array[Double]])] = chunks.zipWithIndex
      .map{case (record, idx) => ((idx.toFloat / samplingRate), record)}.toArray

    val segmented = records.map{
      case (idx, channels) => (idx, channels.map(segmentationClass.compute))
    }

    val ffts = segmented.map{
      case (idx, channels) => (idx, channels.map(segments => segments.map(fftClass.compute)))
    }

    val periodograms = ffts.map{
      case (idx, channels) => (idx, channels.map(segments => segments.map(periodogramClass.compute)))
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
