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

import org.apache.hadoop.io.{DoubleWritable, LongWritable}
import org.ode.hadoop.io.{TwoDDoubleArrayWritable, WavPcmInputFormat}
import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

import org.ode.engine.io.WavReader
import org.ode.engine.signal_processing._

/**
  * Class that provides a simple signal processing workflow without using Spark.
  *
  * Author: Alexandre Degurse
  */


class SampleWorkflow (
  val spark: SparkSession,
  val soundUrl: URL,
  val recordSize: Int,
  val nfft: Int,
  val winSize: Int,
  val offset: Int,
  val soundSamplingRate: Float,
  val soundChannels: Int,
  val soundDurationInSecs: Double,
  val soundSampleSizeInBits: Int,
  val slices: Int,
  val lastRecordAction: String = "skip"
) extends Serializable {

  @transient val sc = spark.sparkContext
  @transient val hadoopConf = sc.hadoopConfiguration

  val frameLength = (soundSamplingRate * soundChannels * soundDurationInSecs).toInt

  WavPcmInputFormat.setSampleRate(hadoopConf, soundSamplingRate)
  WavPcmInputFormat.setChannels(hadoopConf, soundChannels)
  WavPcmInputFormat.setSampleSizeInBits(hadoopConf, soundSampleSizeInBits)
  WavPcmInputFormat.setRecordSizeInFrames(hadoopConf, (frameLength / slices).toInt)
  WavPcmInputFormat.setPartialLastRecordAction(hadoopConf, "skip")


  val records = sc.newAPIHadoopFile[LongWritable, TwoDDoubleArrayWritable, WavPcmInputFormat](
    soundUrl.toURI.toString,
    classOf[WavPcmInputFormat],
    classOf[LongWritable],
    classOf[TwoDDoubleArrayWritable],
    sc.hadoopConfiguration
  ).map{ case (k, v) => (k, v.get.map(_.map(_.asInstanceOf[DoubleWritable].get))) }


  val segmentationClass = new Segmentation(winSize, Some(offset))
  val fftClass = new FFT(nfft)
  val hammingClass = new HammingWindow(winSize, "symmetric")

  val hammingNormalizationFactor = hammingClass.windowCoefficients
    .map(x => x*x).foldLeft(0.0)((acc, v) => acc + v)

  val periodogramClass = new Periodogram(nfft, 1.0 / (soundSamplingRate * hammingNormalizationFactor))
  val welchClass = new WelchSpectralDensity(nfft)
  val energyClass = new Energy(nfft)

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
    case (idx, channels) => (idx, channels.map(energyClass.computeSPLFromPSD))
  }

}
