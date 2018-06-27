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

package org.ode.utils.test

import org.ode.engine.workflows.{SegmentedRecord, AggregatedRecord}

/**
 * Metric error functions
 *
 * @author Joseph Allemandou, Alexandre Degurse
 */
object ErrorMetrics {

  /**
   * Root-mean-square deviation of two doubles.
   * This error metric is symmetric.
   *
   * @param expected The expected value
   * @param actual The actual value
   * @return The root-mean-square deviation of the doubles
   */
  def rmse(expected: Double, actual: Double): Double = {
    math.sqrt(math.pow(expected - actual,2) / 2)
  }

  /**
   * Root-mean-square deviation of two sequences of doubles.
   * This error metric is symmetric.
   *
   * @param expected The sequence of expected values
   * @param actual The sequence of actual values
   * @return The root-mean-square deviation of the sequences
   */
  def rmse(expected: Seq[Double], actual: Seq[Double]): Double = {
    if (expected.length != actual.length) {
      throw new IllegalArgumentException("The given sequences' sizes don't match")
    }

    val errs = expected.zip(actual).map { case (v1, v2) => math.pow(v1 - v2, 2) }
    val mse = errs.sum / errs.length
    math.sqrt(mse)
  }

  /**
   * Root-mean-square deviation of two records.
   * This error metric is symmetric.
   *
   * @param expected The expected Record
   * @param actual The actual Record
   * @return The root-mean-square deviation of the records
   */
  def rmse(expected: => Array[SegmentedRecord], actual: => Array[SegmentedRecord]): Double = {
    if (expected.length != actual.length) {
      throw new IllegalArgumentException("The given sequences' sizes don't match")
    }

    // scalastyle:off while var.local
    var mse = 0.0

    val numRecords = expected.length
    val numChannels = expected(0)._2.length
    val numSegments = expected(0)._2(0).length
    val segmentLength = expected(0)._2(0)(0).length

    var r = 0
    var c = 0
    var s = 0

    while (r < numRecords) {
      // records keys should be equal
      if (expected(r)._1 != actual(r)._1) {
        throw new IllegalArgumentException("The given records' keys don't match")
      }
      // records should have the same number of channels
      if ((actual(r)._2.length != numChannels) && (expected(r)._2.length != numChannels)) {
        throw new IllegalArgumentException("The given records' keys don't match")
      }

      while (c < numChannels) {
        // each record should have the same number of segments
        if ((actual(r)._2(c).length != numSegments) && (expected(r)._2(c).length != numSegments)) {
          throw new IllegalArgumentException("The given records' number of segment don't match")
        }

        while (s < numSegments) {
          // segments should have the same length, rmse on them ensures it
          // finally compare values
          mse += math.pow(rmse(expected(r)._2(c)(s), actual(r)._2(c)(s)), 2) / segmentLength
          s += 1
        }
        c += 1
      }
      r += 1
    }
    // scalastyle:on while var.local
    math.sqrt(mse / (numChannels * numRecords * numSegments))
  }

  /**
   * Root-mean-square deviation of two AggregatedRecords.
   * This error metric is symmetric.
   *
   * @param expected The expected AggregatedRecord
   * @param actual The actual AggregatedRecord
   * @return The root-mean-square deviation of the AggregatedRecords.
   */
  def rmse(expected: Array[AggregatedRecord], actual: Array[AggregatedRecord]): Double = {
    if (expected.length != actual.length) {
      throw new IllegalArgumentException("The given sequences' sizes don't match")
    }

    // scalastyle:off while var.local
    var mse = 0.0

    val numRecords = expected.length
    val numChannels = expected(0)._2.length
    val recordLength = expected(0)._2(0).length

    var r = 0
    var c = 0

    while (r < numRecords) {
      // records keys should be equal
      if (expected(r)._1 != actual(r)._1) {
        throw new IllegalArgumentException("The given records' keys don't match")
      }

      // records should have the same number of channels
      if ((actual(r)._2.length != numChannels) && (expected(r)._2.length != numChannels)) {
        throw new IllegalArgumentException("The given records' keys don't match")
      }

      while (c < numChannels) {
        // each record should have the same number of segments
        if ((actual(r)._2(c).length != recordLength) && (expected(r)._2(c).length != recordLength)){
          throw new IllegalArgumentException("The given records' length don't match")
        }

        mse += math.pow(rmse(expected(r)._2(c), actual(r)._2(c)), 2) / recordLength
        c += 1
      }
      r += 1
    }
    // scalastyle:on while var.local
    math.sqrt(mse / (numChannels * numRecords))
  }
}
