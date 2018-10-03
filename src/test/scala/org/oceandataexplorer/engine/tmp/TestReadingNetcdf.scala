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

import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._
import scala.reflect.runtime.universe._
import ucar.nc2.NetcdfFile
import ucar.ma2.Section

import org.scalatest.{FlatSpec, Matchers}
import com.holdenkarau.spark.testing.SharedSparkContext
import org.oceandataexplorer.utils.test.OdeCustomMatchers

/**
 * Class providing examples on read netcdf files
 *
 * @author Alexandre Degurse
 */
class TestReadingNetcdf extends FlatSpec
  with Matchers
  with OdeCustomMatchers
  with SharedSparkContext
{

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

    val expectedWind = (0 until 20 by 1).toArray.flatMap(t =>
      (-10.0f to -1.0f by 1.0f).toArray.map(lon =>
        (1.0f to 10.0f by 1.0f).toArray.map(lat =>
          math.cos(t) * math.sin(lat * lon)
        )
      )
    ).flatten

    val wind = windIntensity
      .flatMap(timeSlice => timeSlice.map(lonSlice => lonSlice.map(_.toDouble)))
      .flatten

    // high rmse because values were saved as Float
    wind should rmseMatch(expectedWind)
  }

  "test.nc" should "be h5spark style readable" in {
    val spark = SparkSession.builder.getOrCreate
    val sc = spark.sparkContext

    val testNcFilePath = getClass.getResource("/netcdf/test.nc").toString

    val ncf = NetcdfFile.open(testNcFilePath)
    val vars = ncf.getRootGroup.getVariables.asScala
    val windIntensity = vars.find(v => v.getName == "wind_intensity").get

    // in this example, dims = Array(20, 10, 10)
    val dims = windIntensity.getShape

    val numPartitions = 5

    val wind = sc.range(0, dims(0), 1, numPartitions).flatMap{i =>
      val s = new Section({
        val start = Array.ofDim[Int](dims.length)
        start(0) = i.toInt
        start},{
        val range = Array.ofDim[Int](dims.length)
        dims.copyToArray(range)
        range(0) = 1
        range}
      )

      // Netcdf-java lib classes' are not serializable ...
      val windObject = NetcdfFile.open(testNcFilePath)
        .getRootGroup.getVariables.asScala
        .find(v => v.getName == "wind_intensity").get
        .read(s)
        .copyToNDJavaArray
        // type information with .getClass.getTypeName

      // define the TypeTag of the read object at compile time
      // next step is to find a way to instantiate it dynamically
      val typetag = typeTag[Array[Array[Array[Float]]]]
      def cast[A](a: Any, tt: TypeTag[A]): A = a.asInstanceOf[A]

      cast(windObject, typetag)
    }
    .collect()
    .flatMap(timeSlice => timeSlice.map(lonSlice => lonSlice.map(_.toDouble)))
    .flatten

    val expectedWind = (0 until 20 by 1).toArray.flatMap(t =>
      (-10.0f to -1.0f by 1.0f).toArray.map(lon =>
        (1.0f to 10.0f by 1.0f).toArray.map(lat =>
          math.cos(t) * math.sin(lat * lon)
        )
      )
    ).flatten

    wind should rmseMatch(expectedWind)
  }
}
