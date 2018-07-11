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

package org.oceandataexplorer.perf

import org.scalatest.{Assertion, Matchers, FlatSpec}

/**
 * Trait facilitating building homogeneous performance tests.
 * Tests extending this trait print reports over stdout and check
 * that f1 and f2 applied to the same data provide the same result.
 *
 * @author Joseph Allemandou
 */
trait PerfSpec[T1, T2, U] extends FlatSpec with Matchers {

  val enabled = false
  //val enabled = true

  val d1: T1
  val d2: T2
  val f1: (T1 => U)
  val f2: (T2 => U)
  val f1Desc: String
  val f2Desc: String
  val equalCheck:((U, U) => Assertion) = (r1, r2) => r1 should equal(r2)

  val nbRuns: Int
  val loopsInRun: Int

  private def comparePerf: Seq[(Long, Long)] = {

    (1 to nbRuns).foldLeft(Seq.empty[(Long, Long)])((acc, _) => {

      val f1Before = System.currentTimeMillis()
      var f1Res = f1(d1)
      (1 to loopsInRun).foreach(_ => {
        f1Res = f1(d1)
      })
      val f1After = System.currentTimeMillis()
      val f1Duration = f1After - f1Before

      val f2Before = System.currentTimeMillis()
      var f2Res = f2(d2)
      (1 to loopsInRun).foreach(_ => {
        f2Res = f2(d2)
      })
      val f2After = System.currentTimeMillis()
      val f2Duration = f2After - f2Before

      equalCheck(f1Res, f2Res)

      acc :+ (f1Duration, f2Duration)
    })
  }

  private def average(values: Seq[Long]): Double = {
    values.sum.toDouble / values.size.toDouble
  }

  private def variance(values: Seq[Long]) : Double = {
    val valuesAvg = average(values)
    val sumOfSquares = values.foldLeft(0.0d)((total,value)=>{
      val square = math.pow(value.toDouble - valuesAvg, 2)
      total + square
    })
    sumOfSquares / values.size.toDouble
  }

  private def stdDev(values: Seq[Long]): Double = {
    math.sqrt(variance(values))
  }

  private def isSorted(values: Seq[Long]): Boolean = values match {
    case Seq() => true
    case Seq(_) => true
    case _ => values.sliding(2).forall { case List(v1, v2) => v1 <= v2 }
  }


  private def percentile(p: Double)(sortedSeq: Seq[Long]) = {
    require(0.0 <= p && p <= 1)
    require(isSorted(sortedSeq))
    val k = math.ceil((sortedSeq.length - 1) * p).toInt
    sortedSeq(k)
  }

  private def printReport(durations: Seq[(Long, Long)]): Unit = {

    val f1Durations = durations.map(_._1)
    val f1DurationsSorted = f1Durations.sorted
    val f2Durations = durations.map(_._2)
    val f2DurationsSorted = f2Durations.sorted

    val f1TotalTime = f1Durations.sum
    val f2TotalTime = f2Durations.sum
    val totalTime = f1TotalTime + f2TotalTime

    val percentile25 = percentile(0.25d) _
    val percentile50 = percentile(0.5d) _
    val percentile75 = percentile(0.75d) _
    val percentile90 = percentile(0.9d) _
    val percentile95 = percentile(0.95d) _
    val percentile99 = percentile(0.99d) _

    val runsDiff = durations.map{case (f1Dur, f2Dur) => f1Dur - f2Dur}
    val f1Faster = runsDiff.filter(_ < 0).map(- _)
    val f2Faster = runsDiff.filter(_ > 0)

    println(f"""
       |Comparing $f1Desc%s and $f2Desc%s
       |  (Time in milliseconds)
       |
       |** Durations
       |
       |Total
       |  f1         $f1TotalTime%s (${f1TotalTime * 100 / totalTime}%s %%)
       |  f2         $f2TotalTime%s (${f2TotalTime * 100 / totalTime}%s %%)
       |
       |Average
       |  f1         ${average(f1Durations)}%.3f (stddev: ${stdDev(f1Durations)}%.3f)
       |  f2         ${average(f2Durations)}%.3f (stddev: ${stdDev(f1Durations)}%.3f)
       |
       |Percentiles
       |                  f1            f2
       |  25         ${percentile25(f1DurationsSorted)}%9s     ${percentile25(f2DurationsSorted)}%9s
       |  50         ${percentile50(f1DurationsSorted)}%9s     ${percentile50(f2DurationsSorted)}%9s
       |  75         ${percentile75(f1DurationsSorted)}%9s     ${percentile75(f2DurationsSorted)}%9s
       |  90         ${percentile90(f1DurationsSorted)}%9s     ${percentile90(f2DurationsSorted)}%9s
       |  95         ${percentile95(f1DurationsSorted)}%9s     ${percentile95(f2DurationsSorted)}%9s
       |  99         ${percentile99(f1DurationsSorted)}%9s     ${percentile99(f2DurationsSorted)}%9s
       |
       |
       |** Runs comparisons
       |
       |f1 faster
       |             ${f1Faster.size}%s runs (${f1Faster.size * 100 / nbRuns}%s %%)
       |             By ${average(f1Faster)}%.3f in average (stddev: ${stdDev(f1Faster)}%.3f)
       |f2 faster
       |             ${f2Faster.size}%s runs (${f2Faster.size * 100 / nbRuns}%s %%)
       |             By ${average(f2Faster)}%.3f in average (stddev: ${stdDev(f2Faster)}%.3f)
       |
     """.stripMargin)

  }

  if (enabled) {
    it should s"print performance report:" in {
      printReport(comparePerf)
    }
  }
}
