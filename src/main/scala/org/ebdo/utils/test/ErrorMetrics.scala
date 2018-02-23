/** Copyright (C) 2017 Project-EBDO
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

package org.ebdo.utils.test;


import scala.math.{pow,sqrt};

/**
  * Metric error functions
  * Author: Joseph Allemandou, Alexandre Degurse
  */


object ErrorMetrics {

  /**
  * Function that returns the root-mean-square deviation of two doubles
  * @param expected
  * @param actual
  * This error metric is symmetric
  * @return the root-mean-square deviation of the doubles
  */
  def rmse(expected: Double, actual: Double): Double = {
    return sqrt(pow(expected - actual,2) / 2)
  }

  /**
  * Function that returns the root-mean-square deviation of two sequences of double
  * @param expected
  * @param actual
  * This error metric is symmetric
  * @return the root-mean-square deviation of the sequences
  */
  def rmse(expected: Seq[Double], actual: Seq[Double]): Double = {
    if (expected.length != actual.length)
      throw new IllegalArgumentException("The given sequences' sizes don't match")

    val errs = expected.zip(actual).map { case (v1, v2) => pow(v1-v2, 2) }
    val mse = errs.sum / errs.length
    return sqrt(mse)
  }
}
