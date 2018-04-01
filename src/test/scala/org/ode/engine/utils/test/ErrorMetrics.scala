/** Copyright (C) 2017 Project-ODE
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

import scala.math.abs
import org.scalatest.{FlatSpec, Matchers}

/**
  * Tests for Metric error functions
  * Author: Alexandre Degurse
  */


class TestErrorMetrics extends FlatSpec with Matchers {

  "ErrorMetrics" should "compute the rmse for two sequences" in {
    val seqA = Seq(
      0.1221174368357885, 0.8030612655311997, 0.8732426284336273,
      0.8000925604778708, 0.6351656368136573, 0.323284190497698 ,
      0.6192489942098376, 0.9573403084388671, 0.7101131243855894,
      0.0232360227774637
    )
    val seqB = Seq(
      0.7363374103655478, 0.352353350406777 , 0.7134586729011047,
      0.0323479482933672, 0.9458845231585757, 0.7822982947818798,
      0.844976605688674 , 0.5908107704086722, 0.570884102351707 ,
      0.1368780976139283
    )
    val expectedRMSE = 0.41475327309957516
    val rmseComputed = ErrorMetrics.rmse(seqA,seqB)

    abs(rmseComputed - expectedRMSE) should be < (1e-10)
  }

  "ErrorMetrics" should "compute the rmse for two doubles" in {
    val a = 0.9458845231585757
    val b = 0.323284190497698

    val expectedRMSE = 0.440244917193507
    val rmseComputed = ErrorMetrics.rmse(a,b)

    abs(rmseComputed - expectedRMSE) should be < (1e-10)
  }

  "ErrorMetrics" should "fail when sequences size don't match" in {
    val seqA = Seq(0.1221174368357885, 0.8030612655311997, 0.8732426284336273)
    val seqB = Seq(0.7363374103655478, 0.352353350406777)

    an [IllegalArgumentException] should be thrownBy ErrorMetrics.rmse(seqA,seqB)
  }

}
