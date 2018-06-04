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

package org.ode.engine.example

import java.time.temporal.ChronoUnit
import java.time._

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.scalatest.{Matchers, BeforeAndAfterEach, FlatSpec}

/**
  * Tests for Timeserie generator
  * Author: Joseph Allemandou
  */
class TestTimeserieGenerator
    extends FlatSpec
    with Matchers
    with SharedSparkContext
    with BeforeAndAfterEach
    with RDDComparisons {

  // Global variable to clean execution context for every test
  var timeserieGenerator = null.asInstanceOf[TimeserieGenerator]
  var now = null.asInstanceOf[LocalDateTime]

  override def beforeEach(): Unit = {
    val spark = SparkSession.builder.getOrCreate
    timeserieGenerator = new TimeserieGenerator(spark)
    now = LocalDateTime.ofInstant(Instant.now(), ZoneOffset.UTC)
  }

  "TimeseriesGenerationExample" should "return a dataframe when given correct inputs with 1 partition" in {

    val resultDf = timeserieGenerator.makeTimeserie(
      now,                                 // from now
      now.plus(1, ChronoUnit.HOURS),       // to now + 1h
      Duration.of(1, ChronoUnit.SECONDS)   // every second
    )

    val expectedSchema = StructType(Seq(
      StructField("ts", TimestampType, nullable = true),
      StructField("val", DoubleType, nullable = false)
    ))

    resultDf.schema shouldEqual expectedSchema

    val result = resultDf.rdd.map(row => (row.getTimestamp(0), row.getDouble(1)))

    val expectedResult = sc.parallelize((0 to 3599)
      .map(idx => {
        val ts = timeserieGenerator.dt2ts(now.plus(idx, ChronoUnit.SECONDS))
        (ts, idx.toDouble)
      }))

    assertRDDEquals(expectedResult, result)
  }

  it should "return a dataframe when given correct inputs with 2 partitions" in {

    val resultDf = timeserieGenerator.makeTimeserie(
      now,                                 // from now
      now.plus(1, ChronoUnit.HOURS),       // to now + 1h
      Duration.of(1, ChronoUnit.SECONDS),  // every second
      numPartitions = 2                    // over 2 partitions
    )

    val expectedSchema = StructType(Seq(
      StructField("ts", TimestampType, nullable = true),
      StructField("val", DoubleType, nullable = false)
    ))

    resultDf.schema shouldEqual expectedSchema

    val result = resultDf.rdd.map(row => (row.getTimestamp(0), row.getDouble(1)))
    val expectedResult = sc.parallelize((0 to 1799)
      .flatMap(idx => {
        val ts1 = timeserieGenerator.dt2ts(now.plus(idx, ChronoUnit.SECONDS))
        val ts2 = timeserieGenerator.dt2ts(now.plus(1800 + idx, ChronoUnit.SECONDS))
        Seq((ts1, idx.toDouble), (ts2, idx.toDouble))
      }).sortBy(_._1.getTime))

    assertRDDEquals(expectedResult, result)
  }

  it should "return a dataframe when given correct inputs with 7 partitions" in {

    val resultDf = timeserieGenerator.makeTimeserie(
      now,                                 // from now
      now.plus(1, ChronoUnit.HOURS),       // to now + 1h
      Duration.of(1, ChronoUnit.SECONDS),  // every second
      numPartitions = 7                    // over 7 partitions
    )

    val expectedSchema = StructType(Seq(
      StructField("ts", TimestampType, nullable = true),
      StructField("val", DoubleType, nullable = false)
    ))

    resultDf.schema shouldEqual expectedSchema

    val result = resultDf.rdd.map(row => (row.getTimestamp(0), row.getDouble(1)))

    // 3600 = 514 * 7 + 2
    val expectedResult = sc.parallelize((0 to 513)
      .flatMap(idx => {
        (0 to 6).map(ii => {
          val ts = timeserieGenerator.dt2ts(now.plus(idx + 514 * ii, ChronoUnit.SECONDS))
          (ts, idx.toDouble)
        })
      }) ++ Seq(
        (timeserieGenerator.dt2ts(now.plus(3598, ChronoUnit.SECONDS)), 514.0),
        (timeserieGenerator.dt2ts(now.plus(3599, ChronoUnit.SECONDS)), 515.0)
      ).sortBy(_._1.getTime))

    assertRDDEquals(expectedResult, result)
  }

  it should "fail if tick is smaller than interval" in {
    assertThrows[IllegalArgumentException](
      timeserieGenerator.makeTimeserie(
        now,                                 // from now
        now.plus(1, ChronoUnit.SECONDS),     // to now + 1second
        Duration.of(1, ChronoUnit.HOURS)     // every hour -- FAIL
      ))
  }

  it should "return a dataframe when given field names" in {
    val resultDf = timeserieGenerator.makeTimeserie(
      now,
      now.plus(1, ChronoUnit.SECONDS),
      Duration.of(1, ChronoUnit.SECONDS),
      tsName = "test_ts",                 // new ts name
      valueName = "test_val"              // new val name
    )

    val expectedSchema = StructType(Seq(
      StructField("test_ts", TimestampType, nullable = true),
      StructField("test_val", DoubleType, nullable = false)
    ))

    resultDf.schema shouldEqual expectedSchema

  }
}