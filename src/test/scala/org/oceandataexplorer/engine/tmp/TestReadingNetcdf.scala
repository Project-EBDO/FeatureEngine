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

package org.oceandataexplorer.engine.tmp

import collection.JavaConverters._
import ucar.nc2.NetcdfFile

import org.scalatest.{FlatSpec, Matchers}
import org.oceandataexplorer.utils.test.OdeCustomMatchers

/**
 * Class providing examples on read netcdf files
 *
 * @author Alexandre Degurse
 */
class TestReadingNetcdf extends FlatSpec with Matchers with OdeCustomMatchers {

  /**
   * Maximum error allowed for [[OdeCustomMatchers.RmseMatcher]]
   */
  val maxRMSE = 2.0E-8

  "test.nc" should "be readable" in {
    val testNcFilePath = getClass.getResource("/netcdf/test.nc").toString

    val ncf = NetcdfFile.open(testNcFilePath)
    val vars = ncf.getRootGroup.getVariables.asScala

    val time = vars.find(v => v.getName == "time").get
      .read
      .copyTo1DJavaArray
      .asInstanceOf[Array[Int]]

    val lat = vars.find(v => v.getName == "lat").get
      .read
      .copyTo1DJavaArray()
      .asInstanceOf[Array[Float]]

    val lon = vars.find(v => v.getName == "lon").get
      .read()
      .copyTo1DJavaArray
      .asInstanceOf[Array[Float]]

    val windIntensity = vars.find(v => v.getName == "wind_intensity").get
      .read
      .copyToNDJavaArray
      .asInstanceOf[Array[Array[Array[Float]]]]

    (1.0f to 10.0f by 1.0f).toArray.zip(lat)
      .foreach { case (expected, actual) =>
        actual shouldEqual expected
      }

    (-10.0f to -1.0f by 1.0f).toArray.zip(lon)
      .foreach { case (expected, actual) =>
        actual shouldEqual expected
      }

    (0 until 20 by 1).toArray.zip(time)
      .foreach { case (expected, actual) =>
        actual shouldEqual expected
      }

    val expectedWind = (0 until 20 by 1).toArray.map(t =>
      (-10.0f to -1.0f by 1.0f).toArray.map(lon =>
        (1.0f to 10.0f by 1.0f).toArray.map(lat =>
          math.cos(t) * math.sin(lat * lon)
        )
      )
    ).flatten.flatten

    val wind = windIntensity
      .map(timeSlice => timeSlice.map(lonSlice => lonSlice.map(_.toDouble)))
      .flatten.flatten

    // high rmse because values were saved as Float
    wind should rmseMatch(expectedWind)

  }
}
