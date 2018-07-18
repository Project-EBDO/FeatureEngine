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

import org.scalatest.{FlatSpec, Matchers}


/**
 * Tests showing interesting behavior using floating-point collections
 *
 * @author Joseph Allemandou
 */
class TestFloatingPointRounding extends FlatSpec with Matchers {

  "FloatingPointRounding" should "have exactly same values if sequences are created " +
    "with the same first class" in {
    val d = 0.0d to 1.0d by 0.1d
    val d1 = d.toArray
    val d2 = d.toArray.toVector
    d1.toSeq should equal(d2)
  }

  "FloatingPointRounding" should "not have exactly same values if sequences are created " +
    "with different first classes" in {
    val d = 0.0d to 1.0d by 0.1d
    val d1 = d.toArray
    val d2 = d.toVector
    d1.toSeq should not equal d2
  }

}
