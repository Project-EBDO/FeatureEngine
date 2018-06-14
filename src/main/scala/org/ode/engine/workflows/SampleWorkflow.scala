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
import org.apache.spark.rdd.RDD

import scala.io.Source

import org.apache.hadoop.io.{DoubleWritable, LongWritable}
import org.ode.hadoop.io.{TwoDDoubleArrayWritable, WavPcmInputFormat}
import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

import org.ode.engine.io.WavReader
import org.ode.engine.signal_processing._

/**
  * Simple signal processing workflow in Spark.
  *
  * Author: Alexandre Degurse, Joseph Allemandou
  */

class SampleWorkflow (
                       val spark: SparkSession,
                       val recordDurationInSec: Float,
                       val winSize: Int,
                       val nfft: Int,
                       val fftOffset: Int,
                       val lastRecordAction: String = "skip"
                     ) extends Serializable {

  type Record = (Float, Array[Array[Array[Double]]])
  type AggregatedRecord = (Float, Array[Array[Double]])

  def apply(
             soundUrl: URL,
             soundSamplingRate: Float,
             soundChannels: Int,
             soundSampleSizeInBits: Int
           ): Map[String, Either[RDD[Record], RDD[AggregatedRecord]]] = {

    val recordSizeInFrame = soundSamplingRate * recordDurationInSec

    if (recordSizeInFrame % 1 != 0.0f) {
      throw new IllegalArgumentException(s"Computed record size $recordSizeInFrame should not have a decimal part.")
    }

    val hadoopConf = spark.sparkContext.hadoopConfiguration

    WavPcmInputFormat.setSampleRate(hadoopConf, soundSamplingRate)
    WavPcmInputFormat.setChannels(hadoopConf, soundChannels)
    WavPcmInputFormat.setSampleSizeInBits(hadoopConf, soundSampleSizeInBits)
    WavPcmInputFormat.setRecordSizeInFrames(hadoopConf, recordSizeInFrame.toInt)
    WavPcmInputFormat.setPartialLastRecordAction(hadoopConf, lastRecordAction)

    val records = spark.sparkContext.newAPIHadoopFile[LongWritable, TwoDDoubleArrayWritable, WavPcmInputFormat](
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


    val segmentationClass = new Segmentation(winSize, Some(fftOffset))
    val fftClass = new FFT(nfft)
    // TODO -- Change hammingType to an Enum (less error-prone for an API)
    val hammingClass = new HammingWindow(winSize, "symmetric")

    // TODO -- Is this normalization factor a classic ?
    // If so, maybe a method of hamming or even SpectrogramWindow class?
    val hammingNormalizationFactor = hammingClass.windowCoefficients
      .map(x => x*x).foldLeft(0.0)((acc, v) => acc + v)

    val periodogramClass = new Periodogram(nfft, 1.0 / (soundSamplingRate * hammingNormalizationFactor))
    val welchClass = new WelchSpectralDensity(nfft)
    val energyClass = new Energy(nfft)

    val segmented = records.mapValues(channels => channels.map(segmentationClass.compute))
    val ffts = segmented.mapValues(channels => channels.map(signalSegment => signalSegment.map(fftClass.compute)))
    val periodograms = ffts.mapValues(channels => channels.map(fftSegment => fftSegment.map(periodogramClass.compute)))
    val welchs = periodograms.mapValues(channels => channels.map(welchClass.compute))
    // TODO -- We should rename all PSD functions to more explicit terminology
    // Are we expecting periodograms or welchs in SPL ????
    // I'm assuming it is periodogram, and therefore there was a bug originally
    val spls = periodograms.mapValues(channels =>
      channels.map(periodogramSegment => periodogramSegment.map(energyClass.computeSPLFromPSD)))

    Map(
      "ffts" -> Left(ffts),
      "periodograms" -> Left(periodograms),
      "welchs" -> Right(welchs),
      "spls" -> Right(spls)
    )

  }

}

