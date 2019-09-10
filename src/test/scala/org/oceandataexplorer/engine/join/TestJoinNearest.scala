/** Copyright (C) 2017-2018 Project-ODE
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.0 See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.0 If not, see <http://www.gnu.org/licenses/>.
 */

package org.oceandataexplorer.engine.join


import java.sql.Timestamp
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, SparkSession}

import org.scalatest.{FlatSpec, Matchers}
import com.holdenkarau.spark.testing.SharedSparkContext
import org.oceandataexplorer.utils.test.OdeCustomMatchers

/**
 * JoinNearest test implementation
 *
 * @author Alexandre Degurse
 */
class TestJoinNearest extends FlatSpec with SharedSparkContext with Matchers {

  val toValue = udf[Double, Timestamp]((ts: Timestamp) => ts.toInstant.toEpochMilli.toDouble)

  "JoinNearest" should "join two timeseries with the same timestamps" in {
    val spark = SparkSession.builder.getOrCreate
    val sc = spark.sparkContext

    import spark.implicits._

    val epochToTimestamp = udf[Timestamp, Long]((e: Long) => new Timestamp(e))

    val df1 = sc
      .parallelize((0L until 10000L by 1000L))
      .toDF
      .select(epochToTimestamp($"value") as "timestamp")
      .withColumn("x", toValue($"timestamp"))

    val df2 = sc
      .parallelize((0L until 10000L by 1000L))
      .toDF
      .select(epochToTimestamp($"value") as "timestamp")
      .withColumn("y", toValue($"timestamp"))

    val jn = JoinNearest(spark)
    val dfMerged = jn(df1, df2)

    dfMerged.count shouldBe 10

    val columns = Array("timestamp", "x", "y")

    dfMerged.columns shouldBe columns

    val ts = dfMerged
      .collect
      .map(r => r.getAs[Timestamp](0))
      .map(ts => ts.toInstant.toEpochMilli)
      .toArray

    val expectedTs = (0L until 10000L by 1000L).toArray

    ts.zip(expectedTs)
      .foreach(tup => tup._1 shouldBe tup._2)
  }


  it should "join two timeseries when aux has higher granularity" in {
    val spark = SparkSession.builder.getOrCreate
    val sc = spark.sparkContext

    import spark.implicits._

    val epochToTimestamp = udf[Timestamp, Long]((e: Long) => new Timestamp(e))

    val df1 = sc
      .parallelize((0L until 10000L by 1000L))
      .toDF
      .select(epochToTimestamp($"value") as "timestamp")
      .withColumn("x", toValue($"timestamp"))

    val df2 = sc
      .parallelize((0L until 10000L by 10L))
      .toDF
      .select(epochToTimestamp($"value") as "timestamp")
      .withColumn("y", toValue($"timestamp"))

    val jn = JoinNearest(spark)
    val dfMerged = jn(df1, df2)

    dfMerged.count shouldBe 10

    val columns = Array("timestamp", "x", "y")

    dfMerged.columns shouldBe columns

    val ts = dfMerged
      .collect
      .map(r => r.getAs[Timestamp](0))
      .map(ts => ts.toInstant.toEpochMilli)
      .toArray

    val expectedTs = (0L until 10000L by 1000L).toArray


    ts.zip(expectedTs)
      .foreach(tup => tup._1 shouldBe tup._2)

    val y = dfMerged
      .collect
      .map(r => r.getAs[Double](2))
      .toArray

    val expectedY = (0.0 until 10000.0 by 1000.0).toArray

    y.zip(expectedY)
      .foreach(tup => tup._1 shouldBe tup._2)
  }

  it should "join two timeseries when aux has lower granularity" in {
    val spark = SparkSession.builder.getOrCreate
    val sc = spark.sparkContext

    import spark.implicits._

    val epochToTimestamp = udf[Timestamp, Long]((e: Long) => new Timestamp(e))

    val df1 = sc
      .parallelize((0L until 10000L by 1000L))
      .toDF
      .select(epochToTimestamp($"value") as "timestamp")
      .withColumn("x", toValue($"timestamp"))

    val df2 = sc
      .parallelize((0L to 10000L by 5000L))
      .toDF
      .select(epochToTimestamp($"value") as "timestamp")
      .withColumn("y", toValue($"timestamp"))

    val jn = JoinNearest(spark)
    val dfMerged = jn(df1, df2)

    dfMerged.count shouldBe 10

    val columns = Array("timestamp", "x", "y")

    dfMerged.columns shouldBe columns

    val ts = dfMerged
      .collect
      .map(r => r.getAs[Timestamp](0))
      .map(ts => ts.toInstant.toEpochMilli)
      .toArray

    val expectedTs = (0L until 10000L by 1000L).toArray

    ts.zip(expectedTs)
      .foreach(tup => tup._1 shouldBe tup._2)

    val y = dfMerged
      .collect
      .map(r => r.getAs[Double](2))
      .toArray


    val expectedY = Array(
      0.0, 0.0, 0.0, 5000.0, 5000.0,
      5000.0, 5000.0, 5000.0, 10000.0, 10000.0
    )

    y.zip(expectedY)
      .foreach(tup => tup._1 shouldBe tup._2)
  }

  it should "join two timeseries with the same timestamps when time granulity is provided" in {
    val spark = SparkSession.builder.getOrCreate
    val sc = spark.sparkContext

    import spark.implicits._

    val epochToTimestamp = udf[Timestamp, Long]((e: Long) => new Timestamp(e))

    val df1 = sc
      .parallelize((0L until 10000L by 1000L))
      .toDF
      .select(epochToTimestamp($"value") as "timestamp")
      .withColumn("x", toValue($"timestamp"))

    val df2 = sc
      .parallelize((0L until 10000L by 1000L))
      .toDF
      .select(epochToTimestamp($"value") as "timestamp")
      .withColumn("y", toValue($"timestamp"))

    val jn = JoinNearest(spark)
    val dfMerged = jn(df1, df2, Some(1000L))

    dfMerged.count shouldBe 10

    val columns = Array("timestamp", "x", "y")

    dfMerged.columns shouldBe columns

    val ts = dfMerged
      .collect
      .map(r => r.getAs[Timestamp](0))
      .map(ts => ts.toInstant.toEpochMilli)
      .toArray

    val expectedTs = (0L until 10000L by 1000L).toArray

    ts.zip(expectedTs)
      .foreach(tup => tup._1 shouldBe tup._2)
  }


  it should "join two timeseries when aux has higher granularity and time granulity is provided" in {
    val spark = SparkSession.builder.getOrCreate
    val sc = spark.sparkContext

    import spark.implicits._

    val epochToTimestamp = udf[Timestamp, Long]((e: Long) => new Timestamp(e))

    val df1 = sc
      .parallelize((0L until 10000L by 1000L))
      .toDF
      .select(epochToTimestamp($"value") as "timestamp")
      .withColumn("x", toValue($"timestamp"))

    val df2 = sc
      .parallelize((0L until 10000L by 10L))
      .toDF
      .select(epochToTimestamp($"value") as "timestamp")
      .withColumn("y", toValue($"timestamp"))

    val jn = JoinNearest(spark)
    val dfMerged = jn(df1, df2, Some(10L))

    dfMerged.count shouldBe 10

    val columns = Array("timestamp", "x", "y")

    dfMerged.columns shouldBe columns

    val ts = dfMerged
      .collect
      .map(r => r.getAs[Timestamp](0))
      .map(ts => ts.toInstant.toEpochMilli)
      .toArray

    val expectedTs = (0L until 10000L by 1000L).toArray


    ts.zip(expectedTs)
      .foreach(tup => tup._1 shouldBe tup._2)

    val y = dfMerged
      .collect
      .map(r => r.getAs[Double](2))
      .toArray

    val expectedY = (0.0 until 10000.0 by 1000.0).toArray

    y.zip(expectedY)
      .foreach(tup => tup._1 shouldBe tup._2)
  }

  it should "join two timeseries when aux has lower granularity when time granulity is provided" in {
    val spark = SparkSession.builder.getOrCreate
    val sc = spark.sparkContext

    import spark.implicits._

    val epochToTimestamp = udf[Timestamp, Long]((e: Long) => new Timestamp(e))

    val df1 = sc
      .parallelize((0L until 10000L by 1000L))
      .toDF
      .select(epochToTimestamp($"value") as "timestamp")
      .withColumn("x", toValue($"timestamp"))

    val df2 = sc
      .parallelize((0L to 10000L by 5000L))
      .toDF
      .select(epochToTimestamp($"value") as "timestamp")
      .withColumn("y", toValue($"timestamp"))

    val jn = JoinNearest(spark)
    val dfMerged = jn(df1, df2, Some(5000L))

    dfMerged.count shouldBe 10

    val columns = Array("timestamp", "x", "y")

    dfMerged.columns shouldBe columns

    val ts = dfMerged
      .collect
      .map(r => r.getAs[Timestamp](0))
      .map(ts => ts.toInstant.toEpochMilli)
      .toArray

    val expectedTs = (0L until 10000L by 1000L).toArray

    ts.zip(expectedTs)
      .foreach(tup => tup._1 shouldBe tup._2)

    val y = dfMerged
      .collect
      .map(r => r.getAs[Double](2))
      .toArray


    val expectedY = Array(
      0.0, 0.0, 0.0, 5000.0, 5000.0,
      5000.0, 5000.0, 5000.0, 10000.0, 10000.0
    )

    y.zip(expectedY)
      .foreach(tup => tup._1 shouldBe tup._2)
  }

  it should "raise an IllegalArgumentException when given a negative time granularity" in {
    val spark = SparkSession.builder.getOrCreate
    val sc = spark.sparkContext

    import spark.implicits._

    val epochToTimestamp = udf[Timestamp, Long]((e: Long) => new Timestamp(e))

    val df = sc
      .parallelize((0L until 10000L by 1000L))
      .toDF
      .select(epochToTimestamp($"value") as "timestamp")
      .withColumn("x", toValue($"timestamp"))

    val jn = JoinNearest(spark)

    an[IllegalArgumentException] should be thrownBy jn(df, df, Some(-1L))
    an[IllegalArgumentException] should be thrownBy jn(df, df, Some(0))
  }
}
