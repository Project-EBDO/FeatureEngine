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
import org.scalatest.{FlatSpec, Matchers}

/**
 * Tests for Metric error functions
 *
 * @author Alexandre Degurse
 */
class TestErrorMetrics extends FlatSpec with Matchers {

  "ErrorMetrics" should "compute the rmse for two arrays" in {
    val arrA = Array(
      0.1221174368357885, 0.8030612655311997, 0.8732426284336273,
      0.8000925604778708, 0.6351656368136573, 0.323284190497698 ,
      0.6192489942098376, 0.9573403084388671, 0.7101131243855894,
      0.0232360227774637
    )
    val arrB = Array(
      0.7363374103655478, 0.352353350406777 , 0.7134586729011047,
      0.0323479482933672, 0.9458845231585757, 0.7822982947818798,
      0.844976605688674 , 0.5908107704086722, 0.570884102351707 ,
      0.1368780976139283
    )
    val expectedRMSE = 0.41475327309957516
    val rmseComputed = ErrorMetrics.rmse(arrA, arrB)

    math.abs(rmseComputed - expectedRMSE) should be < 1e-10
  }

  it should "compute the rmse for two doubles" in {
    val a = 0.9458845231585757
    val b = 0.323284190497698

    val expectedRMSE = 0.440244917193507
    val rmseComputed = ErrorMetrics.rmse(a,b)

    math.abs(rmseComputed - expectedRMSE) should be < 1e-10
  }

  it should "compute the rmse for two AggregatedRecord" in {
    val aggRecA: Array[AggregatedRecord] = Array(
      (
        100L,
        Array(
          Array(
            0.1221174368357885, 0.8030612655311997, 0.8732426284336273,
            0.8000925604778708, 0.6351656368136573, 0.323284190497698 ,
            0.6192489942098376, 0.9573403084388671, 0.7101131243855894,
            0.0232360227774637
          )
        )
      )
    )

    val aggRecB: Array[AggregatedRecord] = Array(
      (
        100L,
        Array(
          Array(
            0.7363374103655478, 0.352353350406777 , 0.7134586729011047,
            0.0323479482933672, 0.9458845231585757, 0.7822982947818798,
            0.844976605688674 , 0.5908107704086722, 0.570884102351707 ,
            0.1368780976139283
          )
        )
      )
    )

    val expectedRMSE = 0.41475327309957516
    val rmseComputed = ErrorMetrics.rmse(aggRecA, aggRecB)

    math.abs(rmseComputed - expectedRMSE) should be < 1e-10
  }

  it should "compute the rmse for two Record" in {
    val segRecA: Array[SegmentedRecord] = Array(
      (
        100L,
        Array(
          Array(
            Array(
              0.1221174368357885, 0.8030612655311997, 0.8732426284336273,
              0.8000925604778708, 0.6351656368136573, 0.323284190497698 ,
              0.6192489942098376, 0.9573403084388671, 0.7101131243855894,
              0.0232360227774637
            )
          )
        )
      )
    )

    val segRecB: Array[SegmentedRecord] = Array(
      (
        100L,
        Array(
          Array(
            Array(
              0.7363374103655478, 0.352353350406777 , 0.7134586729011047,
              0.0323479482933672, 0.9458845231585757, 0.7822982947818798,
              0.844976605688674 , 0.5908107704086722, 0.570884102351707 ,
              0.1368780976139283
            )
          )
        )
      )
    )

    val expectedRMSE = 0.41475327309957516
    val rmseComputed = ErrorMetrics.rmse(segRecA, segRecB)

    math.abs(rmseComputed - expectedRMSE) should be < 1e-10
  }

  it should "raise an IllegalArgumentException when sequences with differente sizes" in {
    val arrA = Array(0.1221174368357885, 0.8030612655311997, 0.8732426284336273)
    val arrB = Array(0.7363374103655478, 0.352353350406777)

    the[IllegalArgumentException] thrownBy {
      ErrorMetrics.rmse(arrA, arrB)
    } should have message "The given sequences' sizes don't match"
  }

  it should "raise an IllegalArgumentException when the given SegmentedRecord keys don't match" in {
    val segRecA: Array[SegmentedRecord] = Array((101L, Array(Array(Array(1.0)))))
    val segRecB: Array[SegmentedRecord] = Array((100L, Array(Array(Array(1.0)))))

    the[IllegalArgumentException] thrownBy {
      ErrorMetrics.rmse(segRecA, segRecB)
    } should have message "The given records' keys don't match"
  }

  it should "raise an IllegalArgumentException when the given SegmentedRecord segment length don't match" in {
    val segRecA: Array[SegmentedRecord] = Array((100L, Array(Array(Array(1.0)))))
    val segRecB: Array[SegmentedRecord] = Array((100L, Array(Array(Array(1.0, 2.0)))))

    the[IllegalArgumentException] thrownBy {
      ErrorMetrics.rmse(segRecA, segRecB)
    } should have message "The given sequences' sizes don't match"
  }

  it should "raise an IllegalArgumentException when the given SegmentedRecord number of channels don't match" in {
    val segRecA: Array[SegmentedRecord] = Array((100L, Array(Array(Array(1.0)))))
    val segRecB: Array[SegmentedRecord] = Array((100L, Array(Array(Array(1.0)), Array(Array(1.0)))))

    the[IllegalArgumentException] thrownBy {
      ErrorMetrics.rmse(segRecA, segRecB)
    } should have message "The given records' number of channels don't match"
  }

  it should "raise an IllegalArgumentException when the given SegmentedRecord number of segments don't match" in {
    val segRecA: Array[SegmentedRecord] = Array((100L, Array(Array(Array(1.0)))))
    val segRecB: Array[SegmentedRecord] = Array((100L, Array(Array(Array(1.0), Array(2.0)))))

    the[IllegalArgumentException] thrownBy {
      ErrorMetrics.rmse(segRecA, segRecB)
    } should have message "The given records' number of segment don't match"
  }



  it should "raise an IllegalArgumentException when the given AggregatedRecord keys don't match" in {
    val aggRecA: Array[AggregatedRecord] = Array((101L, Array(Array(1.0))))
    val aggRecB: Array[AggregatedRecord] = Array((100L, Array(Array(1.0))))

    the[IllegalArgumentException] thrownBy {
      ErrorMetrics.rmse(aggRecA, aggRecB)
    } should have message "The given records' keys don't match"
  }

  it should "raise an IllegalArgumentException when the given AggregatedRecord number of channels don't match" in {
    val aggRecA: Array[AggregatedRecord] = Array((100L, Array(Array(1.0))))
    val aggRecB: Array[AggregatedRecord] = Array((100L, Array(Array(1.0), Array(2.0))))

    the[IllegalArgumentException] thrownBy {
      ErrorMetrics.rmse(aggRecA, aggRecB)
    } should have message "The given records' number of channels don't match"
  }

  it should "raise an IllegalArgumentException when the given AggregatedRecord segment length don't match" in {
    val aggRecA: Array[AggregatedRecord] = Array((100L, Array(Array(1.0))))
    val aggRecB: Array[AggregatedRecord] = Array((100L, Array(Array(1.0, 2.0))))

    the[IllegalArgumentException] thrownBy {
      ErrorMetrics.rmse(aggRecA, aggRecB)
    } should have message "The given records' length don't match"
  }

  it should "raise an IllegalArgumentException when given unsupported result type" in {
    val result = Array("Definitely a wrong type for RMSE")

    the[IllegalArgumentException] thrownBy {
      ErrorMetrics.rmse(result, result)
    } should have message "Unsupported type (Array[java.lang.String]) for RMSE " +
      "(must be either Double, Array[Double], Array[SegmentedRecord], Array[AggregatedRecord])"
  }
}
