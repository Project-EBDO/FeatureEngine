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
import org.oceandataexplorer.engine.workflows.{AggregatedRecord, SegmentedRecord}

/**
 * Tests for OdeCustomMatchers
 *
 * @author Alexandre Degurse
 */
class TestOdeCustomMatchers extends FlatSpec with Matchers with OdeCustomMatchers {

  /**
   * Maximum error allowed for [[OdeCustomMatchers.RmseMatcher]]
   */
  val maxRMSE = 1.0E-16

  "rmseMatcher" should "should find match when given the same array" in {
    val data = Array(0.1221174368357885, 0.8030612655311997)

    data should rmseMatch(data)
  }

  "rmseMatcher" should "should find match when given the same double" in {
    val data = 0.1

    data should rmseMatch(data)
  }

  "rmseMatcher" should "should find match when given the same segmented result record" in {
    val data: Array[SegmentedRecord] = Array((1L, Array(Array(Array(1.0)))))

    data should rmseMatch(data)
  }

  "rmseMatcher" should "should find match when given the same aggregated result record" in {
    val data: Array[AggregatedRecord] = Array((1L, Array(Array(1.0))))

    data should rmseMatch(data)
  }
}
