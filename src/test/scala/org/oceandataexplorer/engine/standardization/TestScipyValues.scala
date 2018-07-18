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

package org.oceandataexplorer.engine.standardization

import org.oceandataexplorer.utils.test.OdeCustomMatchers
import org.scalatest.{FlatSpec, Matchers}

/**
 * Automated signal processing tests for Scipy reference values
 * @author Alexandre Degurse
 */

class TestScipyValues extends FlatSpec with Matchers with OdeCustomMatchers {

  /**
   * Maximum error allowed for [[OdeCustomMatchers.RmseMatcher]]
   */
  val maxRMSE = 1.0E-13

  val refValuesLocation = "/standardization/scipy/values"

  val soundParams = List(
    SoundHandler("Sound1", 64, 24, 9811, 3906.0f, 1),
    SoundHandler("Sound2", 64, 24, 3120, 2000.0f, 1)
  )

  val refFiles: List[ResultsHandler] = List(
    ResultsHandler(soundParams(0), "vTOL", 3906, 3906, 3906, 64, refValuesLocation),
    ResultsHandler(soundParams(1), "vTOL", 2000, 2000, 2000, 64, refValuesLocation),
    ResultsHandler(soundParams(0), "vTOL", 9000, 9000, 9000, 64, refValuesLocation),
    ResultsHandler(soundParams(1), "vTOL", 3000, 3000, 3000, 64, refValuesLocation),

    ResultsHandler(soundParams(1), "vFFT", 128, 128, 128, 64, refValuesLocation),
    ResultsHandler(soundParams(1), "vFFT", 2242, 2047, 2042, 64, refValuesLocation),
    ResultsHandler(soundParams(1), "vFFT", 2242, 1984, 1240, 64, refValuesLocation),


    ResultsHandler(soundParams(0), "vPSD", 128, 128, 128, 64, refValuesLocation),
    ResultsHandler(soundParams(0), "vWelch", 128, 128, 128, 64, refValuesLocation),
    ResultsHandler(soundParams(0), "vPSD", 1024, 1024, 1000, 64, refValuesLocation),
    ResultsHandler(soundParams(0), "vWelch", 1024, 1024, 1000, 64, refValuesLocation),
    ResultsHandler(soundParams(0), "vPSD", 130, 120, 100, 64, refValuesLocation),
    ResultsHandler(soundParams(0), "vWelch", 130, 120, 100, 64, refValuesLocation),

    ResultsHandler(soundParams(1), "vPSD", 128, 128, 128, 64, refValuesLocation),
    ResultsHandler(soundParams(1), "vWelch", 128, 128, 128, 64, refValuesLocation),
    ResultsHandler(soundParams(1), "vPSD", 1024, 1024, 1000, 64, refValuesLocation),
    ResultsHandler(soundParams(1), "vWelch", 1024, 1024, 1000, 64, refValuesLocation),
    ResultsHandler(soundParams(1), "vPSD", 130, 120, 100, 64, refValuesLocation),
    ResultsHandler(soundParams(1), "vWelch", 130, 120, 100, 64, refValuesLocation)
  )

  for (resultFileHandler <- refFiles) {
    it should s"should generate the same result as in ${resultFileHandler.fileName}" in {
      val expectedResult = resultFileHandler.getExpectedValues
      val computedValues = resultFileHandler.getComputedValues

      computedValues.length should be(expectedResult.length)

      computedValues.indices.foreach{i =>
        computedValues(i) should rmseMatch(expectedResult(i))
      }
    }
  }
}
