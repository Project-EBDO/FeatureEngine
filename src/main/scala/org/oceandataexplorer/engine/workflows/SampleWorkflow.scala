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

import java.net.URI

import org.apache.hadoop.io.{DoubleWritable, LongWritable}
import org.oceandataexplorer.hadoop.io.{TwoDDoubleArrayWritable, WavPcmInputFormat}
import org.apache.hadoop.mapreduce.lib.input.FileSplit

import org.apache.spark.rdd.{RDD, HadoopRDD, NewHadoopRDD}
import org.apache.spark.sql.{SparkSession, DataFrame, Row}
import org.apache.spark.sql.types._

import com.github.nscala_time.time.Imports._
import org.joda.time.Days
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
 * @param segmentSize The size of the segments to be generated
 * @param segmentOffset The offset used to segment the signal
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
  val segmentSize: Int,
  val segmentOffset: Int,
  val nfft: Int,
  val lowFreq: Option[Double] = None,
  val highFreq: Option[Double] = None,
  val lastRecordAction: String = "skip"
) {

  /**
   * Function used to read wav files inside a Spark workflow
   *
   * @todo pass sounds path instead of names in soundsNameAndStartDate to avoid duplicate when
   * files have the same name but different path
   * @todo read only the files of the list
   *
   * @param soundsUri URI-like string pointing to the wav files
   * (Unix globbing is allowed, file:///tmp/{sound0,sound1}.wav is a valid soundsUri)
   * @param soundsNameAndStartDate A list containing all files names
   * and their start date as a DateTime
   * @param soundSamplingRate Sound's sampling rate
   * @param soundChannels Sound's number of channels
   * @param soundSampleSizeInBits The number of bits used to encode a sample
   * @return The records that contains wav's data
   */
  def readWavRecords(
    soundsUri: String,
    soundsNameAndStartDate: List[(String, DateTime)],
    soundSamplingRate: Float,
    soundChannels: Int,
    soundSampleSizeInBits: Int
  ): RDD[Record] = {

    val recordSizeInFrame = soundSamplingRate * recordDurationInSec

    if (recordSizeInFrame % 1 != 0.0f) {
      throw new IllegalArgumentException(
        s"Computed record size ($recordSizeInFrame) should not have a decimal part.")}

    val soundNames = soundsNameAndStartDate.map(_._1)
    if (soundNames.length != soundNames.distinct.length) {
      throw new IllegalArgumentException(
        "Sounds list contains duplicate filename entries")}

    val hadoopConf = spark.sparkContext.hadoopConfiguration
    WavPcmInputFormat.setSampleRate(hadoopConf, soundSamplingRate)
    WavPcmInputFormat.setChannels(hadoopConf, soundChannels)
    WavPcmInputFormat.setSampleSizeInBits(hadoopConf, soundSampleSizeInBits)
    WavPcmInputFormat.setRecordSizeInFrames(hadoopConf, recordSizeInFrame.toInt)
    WavPcmInputFormat.setPartialLastRecordAction(hadoopConf, lastRecordAction)

    spark.sparkContext.newAPIHadoopFile[LongWritable, TwoDDoubleArrayWritable, WavPcmInputFormat](
      soundsUri,
      classOf[WavPcmInputFormat],
      classOf[LongWritable],
      classOf[TwoDDoubleArrayWritable],
      hadoopConf)
    .asInstanceOf[NewHadoopRDD[LongWritable, TwoDDoubleArrayWritable]]
    .mapPartitionsWithInputSplit{ (inputSplit, iterator) =>
      val fileName: String = inputSplit.asInstanceOf[FileSplit].getPath.getName
      val startDate = soundsNameAndStartDate.find{case (name, date) => name == fileName}.map(_._2)

      if (startDate.isEmpty) {
        throw new IllegalArgumentException(
          s"Read file $fileName has no startDate in given list")}

      iterator.map{ case (writableOffset, writableSignal) =>
        val offsetInMillis = (startDate.get.instant.millis
          + (1000.0f * writableOffset.get.toFloat / soundSamplingRate).toLong)
        val signal = writableSignal.get.map(_.map(_.asInstanceOf[DoubleWritable].get))
        (offsetInMillis, signal)
      }
    }
    .asInstanceOf[RDD[Record]]
  }

  /**
   * Wrapper function used to read a single file
   *
   * @param soundUri URI pointing to a wav file as a String
   * @param soundStartDate The start date of the recording
   * @param soundSamplingRate Sound's samplingRate
   * @param soundChannels Sound's number of channels
   * @param soundSampleSizeInBits The number of bits used to encode a sample
   * @return The records that contains wav's data
   */
  def readWavRecords(
    soundUri: String,
    soundStartDate: DateTime,
    soundSamplingRate: Float,
    soundChannels: Int,
    soundSampleSizeInBits: Int
  ): RDD[Record] = {
    readWavRecords(
      soundUri,
      List(((new URI(soundUri)).getPath.split("/").last, soundStartDate)),
      soundSamplingRate,
      soundChannels,
      soundSampleSizeInBits
    )
  }

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
   * @return A map that contains all basic features as RDDs
   */
  def apply(
    soundsUri: String,
    soundsNameAndStartDate: List[(String, DateTime)],
    soundSamplingRate: Float,
    soundChannels: Int,
    soundSampleSizeInBits: Int
  ): Map[String, Either[RDD[SegmentedRecord], RDD[AggregatedRecord]]] = {

    val records = readWavRecords(soundsUri,
      soundsNameAndStartDate,
      soundSamplingRate,
      soundChannels,
      soundSampleSizeInBits)

    val segmentationClass = new Segmentation(segmentSize, Some(segmentOffset))
    val fftClass = new FFT(nfft, 1.0f)
    val hammingClass = new HammingWindowFunction(segmentSize, Periodic)
    val hammingNormalizationFactor = hammingClass.densityNormalizationFactor()

    val periodogramClass = new Periodogram(
      nfft, 1.0/(soundSamplingRate*hammingNormalizationFactor), soundSamplingRate)
    val welchClass = new WelchSpectralDensity(nfft, soundSamplingRate)
    val energyClass = new Energy(nfft)

    val ffts = records
      .mapValues(chans => chans.map(segmentationClass.compute))
      .mapValues(segmentedChans => segmentedChans.map(signalSegment =>
        signalSegment.map(hammingClass.applyToSignal)))
      .mapValues(windowedChans => windowedChans.map(windowedChan =>
        windowedChan.map(fftClass.compute)))

    val periodograms = ffts.mapValues(fftChans =>
      fftChans.map(fftChan => fftChan.map(periodogramClass.compute)))

    val welchs = periodograms.mapValues(periodogramChans =>
      periodogramChans.map(welchClass.compute))

    val spls = welchs.mapValues(welchChans => welchChans.map(welchChan =>
      Array(energyClass.computeSPLFromPSD(welchChan))))

    val resultMap = Map("ffts" -> Left(ffts),
      "periodograms" -> Left(periodograms),
      "welchs" -> Right(welchs), "spls" -> Right(spls))

    if (nfft >= soundSamplingRate.toInt) {
      val tolClass = new TOL(nfft, soundSamplingRate, lowFreq, highFreq)
      val tols = welchs.mapValues(welchChans => welchChans.map(tolClass.compute))
      resultMap ++ Map("tols" -> Right(tols))
    } else {resultMap}
  }
}
