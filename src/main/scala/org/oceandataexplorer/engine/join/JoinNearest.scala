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

package org.oceandataexplorer.engine.join

import java.sql.Timestamp

import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Class providing nearest neighbor join method for timeseries.
 *
 * @author Alexandre Degurse
 *
 * @param spark The SparkSession to use for join.
 */
case class JoinNearest (spark: SparkSession) {

  /**
   * Method joining two timeseries using nearest neighbor method.
   * It matches the closest values of df2 into df1.
   * df2 fields are will be null if no value is near the ones in df1,
   * thus this method is not symmetric.
   * The timeseries must contain a timestamp field named "timestamp"
   * and time granularity must be consistent.
   * Time granularity of df2 can be provided to avoid unnecessary sort.
   *
   * @param df1 The timeserie to be joined as a DataFrame
   * @param df2 The timeserie to be joined as a DataFrame
   * @param dt2 The time granularity of df2 in milliseconds as a
   * Option[Long]
   * @return The joined timeserie as a DataFrame
   */
  def apply(df1: DataFrame, df2: DataFrame, dt2: Option[Long] = None): DataFrame = {
    import spark.implicits._

    val timestampToEpoch = udf[Long, Timestamp]((ts: Timestamp) => ts.toInstant.toEpochMilli)

    if (dt2.exists(v => v < 1)) {
      throw new IllegalArgumentException(
        s"Incorrect time granularity (${dt2.get}) for nearest neighbor, must be higher than 1"
      )
    }

    // time granularity of df2, either provided or computed
    val dt = dt2.getOrElse({
      // take 2 consecutive timestamps from df2
      val ts2 = df2.select(timestampToEpoch($"timestamp")).sort($"timestamp").take(2)
      // compute time granularity of df2
      (ts2(1).getAs[Long](0) - ts2(0).getAs[Long](0))
    }).toDouble

    df1.join(df2.withColumnRenamed("timestamp", "ts2"),
      (timestampToEpoch($"timestamp") <= (timestampToEpoch($"ts2") + dt / 2)) &&
      (timestampToEpoch($"timestamp") > (timestampToEpoch($"ts2") - dt / 2)),
      "left"
    ).drop("ts2")
  }
}
