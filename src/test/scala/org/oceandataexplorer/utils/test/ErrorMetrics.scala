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

package org.oceandataexplorer.utils.test

import org.oceandataexplorer.engine.workflows.{SegmentedRecord, AggregatedRecord}
import reflect.runtime.universe._

/**
 * Metric error functions
 *
 * @author Joseph Allemandou, Alexandre Degurse
 */
object ErrorMetrics {

  /**
   * Function flattening an aggregated-result into a simple Array
   * to reuse the rmse computation function
   *
   * @param result The aggregated result to be flattened as an array of AggregatedRecord
   * @return The flattened aggregated result as a Array[Double]
   */
  private def aggregatedResultFlattener(result: Array[AggregatedRecord]): Array[Double] = {
    result.flatMap(segRec => segRec._2.flatMap(chans => chans))
  }

  /**
   * Function flattening an segmented-result into a simple Array
   * to reuse the rmse computation function
   *
   * @param result The segmented result to be flattened as an array of SegmentedRecord
   * @return The flattened segmented result as a Array[Double]
   */
  private def segmentedResultFlattener(result: Array[SegmentedRecord]): Array[Double] = {
    result.flatMap(segRec => segRec._2.flatMap(chans => chans.flatMap(chan => chan)))
  }


  def rmse[T](expected: T, actual: T)(implicit tag: TypeTag[T]): Double = (expected, actual, tag) match {

    case (expected, actual, tag) if tag.tpe =:= typeOf[Double] => {
      math.sqrt(math.pow(expected.asInstanceOf[Double] - actual.asInstanceOf[Double], 2) / 2)
    }

    case (expected, actual, tag) if tag.tpe =:= typeOf[Array[Double]] => {
      val expectedResult = expected.asInstanceOf[Array[Double]]
      val actualResult = actual.asInstanceOf[Array[Double]]

      if (expectedResult.length != actualResult.length) {
        throw new IllegalArgumentException("The given sequences' sizes don't match")
      }

      val errs = expectedResult.zip(actualResult).map { case (v1, v2) => math.pow(v1 - v2, 2) }
      val mse = errs.sum / errs.length
      math.sqrt(mse)
    }


    case (expected, actual, tag) if tag.tpe =:= typeOf[Array[SegmentedRecord]] => {
      val expectedResult = expected.asInstanceOf[Array[SegmentedRecord]]
      val actualResult = actual.asInstanceOf[Array[SegmentedRecord]]

      val recordNumber = expectedResult.length
      val channelNumber = expectedResult(0)._2.length
      val segmentNumber = expectedResult(0)._2(0).length
      val segmentLength = expectedResult(0)._2(0)(0).length

      if (actualResult.length != recordNumber) {
        throw new IllegalArgumentException("The given sequences' sizes don't match")
      }

      expectedResult.zip(actualResult).map{ case (expectedRecord, actualRecord) =>
        // records keys should be equal
        if (expectedRecord._1 != actualRecord._1) {
          throw new IllegalArgumentException("The given records' keys don't match")
        }

        // records should have the same number of channels
        if (actualRecord._2.length != channelNumber) {
          throw new IllegalArgumentException("The given records' number of channels don't match")
        }

        actualRecord._2.map(actualChannel => {
          // each channel should have the same number of segments
          if (actualChannel.length != segmentNumber) {
            throw new IllegalArgumentException("The given records' number of segment don't match")
          }

          actualChannel.map(actualSegment => {
            // segments should have the same length,
            if (actualSegment.length != segmentLength) {
              throw new IllegalArgumentException("The given sequences' sizes don't match")
            }
          })
        })
      }

      val expectedFlattenResult = segmentedResultFlattener(expectedResult)
      val actualFlattentResult = segmentedResultFlattener(actualResult)

      rmse(expectedFlattenResult, actualFlattentResult)
    }

    case (expected, actual, tag) if tag.tpe =:= typeOf[Array[AggregatedRecord]] => {
      val expectedResult = expected.asInstanceOf[Array[AggregatedRecord]]
      val actualResult = actual.asInstanceOf[Array[AggregatedRecord]]

      val recordNumber = expectedResult.length
      val channelNumber = expectedResult(0)._2.length
      val channelLength = expectedResult(0)._2(0).length

      if (actualResult.length != recordNumber) {
        throw new IllegalArgumentException("The given sequences' sizes don't match")
      }

      expectedResult.zip(actualResult).map{ case (expectedRecord, actualRecord) =>
        // records keys should be equal
        if (expectedRecord._1 != actualRecord._1) {
          throw new IllegalArgumentException("The given records' keys don't match")
        }

        // records should have the same number of channels
        if (actualRecord._2.length != channelNumber) {
          throw new IllegalArgumentException("The given records' number of channels don't match")
        }

        actualRecord._2.map(actualChannel => {
          // each channel should have the same length
          if (actualChannel.length != channelLength) {
            throw new IllegalArgumentException("The given records' length don't match")
          }
        })
      }

      val expectedFlattenResult = aggregatedResultFlattener(expectedResult)
      val actualFlattentResult = aggregatedResultFlattener(actualResult)

      rmse(expectedFlattenResult, actualFlattentResult)
    }
  }
}
