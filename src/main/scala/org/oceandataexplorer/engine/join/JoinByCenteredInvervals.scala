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
 * Class providing join method by centered intervals for timeseries.
 *
 * @author Alexandre Degurse
 *
 * @param spark The SparkSession to use for join.
 */
case class JoinByCenteredInvervals (spark: SparkSession) {

  /**
   * Method joining two timeseries using centered interval method.
   * It matches the closest values of toJoinDf into baseDf by performing
   * a left join on centered intervals.
   *
   * @param baseDf The timeserie to be joined as a DataFrame
   * @param toJoinDf The timeserie to be joined as a DataFrame
   * @param toJoinDfIntervalLength The interval length separating two
   * consecutive values of toJoinDf in milliseconds as a Long
   * @return The joined timeserie as a DataFrame
   */
  def apply(
    baseDf: DataFrame,
    toJoinDf: DataFrame,
    toJoinDfIntervalLength: Long,
    baseDfTsColName: Option[String] = None,
    toJoinDfTsColName: Option[String] = None
  ): DataFrame = {
    import spark.implicits._

    val timestampToEpoch = udf[Long, Timestamp]((ts: Timestamp) =>
      ts.toInstant.toEpochMilli
    )

    if (toJoinDfIntervalLength < 1) {
      throw new IllegalArgumentException(
        s"Incorrect time granularity ($toJoinDfIntervalLength)" +
        "for nearest neighbor, must be higher than 1"
      )
    }

    baseDf.join(toJoinDf,
      (timestampToEpoch(baseDf(baseDfTsColName.getOrElse("timestamp")))
        <= (timestampToEpoch(toJoinDf(toJoinDfTsColName.getOrElse("timestamp")))
        + toJoinDfIntervalLength / 2)) &&
      (timestampToEpoch(baseDf(baseDfTsColName.getOrElse("timestamp"))) >
        (timestampToEpoch(toJoinDf(toJoinDfTsColName.getOrElse("timestamp")))
        - toJoinDfIntervalLength / 2)),
      "left"
    ).drop(toJoinDf(toJoinDfTsColName.getOrElse("timestamp")))
  }
}
