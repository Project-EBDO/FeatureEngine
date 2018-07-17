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

import reflect.runtime.universe._
import org.scalatest._
import matchers._

/**
 * ODE Custom matchers for tests
 *
 * @author Alexandre Degurse
 */

trait OdeCustomMatchers {

  /**
   * Maximum error allowed for [[OdeCustomMatchers.RmseMatcher]]
   */
  val maxRMSE: Double

  /**
   * Class providing matcher over RMSE
   * This class uses the maxRMSE value defined above
   *
   * @param expected The expected result for the test
   * @param tag The TypeTag of the generic type T
   * @tparam T The type of data RMSE is computed upon,
   *           accepted types are Double, Array[Double],
   *           Array[SegmentedRecord], Array[AggregatedRecord]
   */
  class RmseMatcher[T](expected: T)(implicit tag: TypeTag[T]) extends Matcher[T] {

    /**
     * apply method for the matcher used to run the test
     *
     * @param actual The actual result
     * @return The result of the test as a MatchResult
     */
    def apply(actual: T): MatchResult = {
      val rmseValue = ErrorMetrics.rmse(actual, expected)
      MatchResult(
        rmseValue < maxRMSE,
        s"The arrays did not rmse-match ($rmseValue > $maxRMSE)",
        s"The arrays did rmse-match (with rmse of $rmseValue)"
      )
    }
  }

  /**
   * Wrapper function for [[RmseMatcher]] instantiation
   *
   * @param expected The expected result for the test
   * @param tag The TypeTag of the generic type T
   * @tparam T The type of data RMSE is computed upon,
   *           accepted types are Double, Array[Double],
   *           Array[SegmentedRecord], Array[AggregatedRecord]
   * @return A new instance of RmseMatcher
   */
  def rmseMatch[T](expected: T)(implicit tag: TypeTag[T]): RmseMatcher[T] = {
    new RmseMatcher[T](expected)(tag)
  }
}
