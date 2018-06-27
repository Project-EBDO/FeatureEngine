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

package org.ode.engine

/**
 * Spark workflow package object providing the types used in Spark workflows.
 */
package object workflows {
  /**
   * Type for signal processing records.
   * A record represents this:
   * (Long: key/timestamp, Array[Array[Array[Double])
   *                          |     |     |> feature
   *                          |     |> segments
   *                          |> channels
   */
  type SegmentedRecord = (Long, Array[Array[Array[Double]]])

  /**
   * Type for signal processing records.
   * A AggregatedRecord represents this:
   * (Long: key/timestamp, Array[Array[Double])
   *                          |     |> feature
   *                          |> channels
   */
  type AggregatedRecord = (Long, Array[Array[Double]])

  /**
   * Type for signal processing records.
   * A AggregatedRecord represents this:
   * (Long: key/timestamp, Array[Array[Double])
   *                          |     |> samples
   *                          |> channels
   */
  type Record = (Long, Array[Array[Double]])
}
