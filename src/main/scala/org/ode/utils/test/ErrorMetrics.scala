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
   * @param actual the actual value
   * @return the root-mean-square deviation of the doubles
   */
  def rmse(expected: Double, actual: Double): Double = {
    math.sqrt(math.pow(expected - actual,2) / 2)
  }

  /**
   * Root-mean-square deviation of two sequences of doubles.
   * This error metric is symmetric.
   *
   * @param expected The sequence of expected values
   * @param actual the sequence of actual values
   * @return the root-mean-square deviation of the sequences
   */
  def rmse(expected: Seq[Double], actual: Seq[Double]): Double = {
    if (expected.length != actual.length) {
      throw new IllegalArgumentException("The given sequences' sizes don't match")
    }

    val errs = expected.zip(actual).map { case (v1, v2) => math.pow(v1 - v2, 2) }
    val mse = errs.sum / errs.length
    math.sqrt(mse)
  }
}
