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

package org.oceandataexplorer.engine.workflows


import org.scalatest.{FlatSpec, Matchers}
import org.oceandataexplorer.utils.test.OdeCustomMatchers
import com.holdenkarau.spark.testing.SharedSparkContext

/**
 * Tests for SampleWorkflow that compares its computations with ScalaSampleWorkflow
 *
 * @author Alexandre Degurse, Joseph Allemandou
 */

class TestScalaSampleWorkflow extends FlatSpec
  with Matchers with SharedSparkContext with OdeCustomMatchers
{
  val maxRMSE = 1.0E-16

  it should "generate the same results as the pure scala workflow" in {
    // Signal processing parameters
    val recordSizeInSec = 0.5f
    val soundSamplingRate = 16000.0f
    val windowSize = 16000
    val windowOverlap = 0
    val nfft = 16000

    // Sound parameters
    val soundUri = getClass.getResource("/wav/sin_16kHz_2.5s.wav").toURI
    val soundChannels = 1
    val soundSampleSizeInBits = 16
    val soundStartDate = "1978-04-11T13:14:20.200Z"


    val scalaWorkflow = new ScalaSampleWorkflow(
      recordSizeInSec,
      windowSize,
      windowOverlap,
      nfft
    )

    val records = scalaWorkflow.readRecords(
      soundUri,
      soundSamplingRate,
      soundChannels,
      soundSampleSizeInBits,
      soundStartDate
    )

    records.length shouldEqual 5
  }
}
