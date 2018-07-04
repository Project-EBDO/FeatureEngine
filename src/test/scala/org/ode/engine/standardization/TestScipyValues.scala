
/** Copyright (C) 2017 Project-ODE
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

package org.ode.engine.standardization

import java.io.File

import org.ode.engine.io.WavReader
import org.ode.engine.signal_processing.{FFT, HammingWindow, Periodogram, Segmentation}

import org.ode.utils.test.ErrorMetrics
import org.scalatest.{FlatSpec, Matchers}

/**
  * Automated signal processing tests for Scipy reference values
  * Author: Alexandre Degurse
  */

class TestScipyValues extends FlatSpec with Matchers {
  val refValuesLocation = "/standardization/scipy/values"

  val soundParams = List(
    SoundParameters("Sound1", 64, 24, 9811, 3906.0, 1),
    SoundParameters("Sound2", 64, 24, 3120, 2000.0, 1)
  )

  val refFiles = List[ResultFileParameters]()

  val maxRMSE = 1.0E-13

  for (resultParam <- refFiles) {
    it should s"should generate the same result as in ${resultParam.fileName}" in {
      val expectedResult = resultParam.getExpectedValues()
      val computedValues = resultParam.computeAlgo()

      computedValues.length should be(expectedResult.length)

      Range(0, computedValues.length).foreach{i =>
        ErrorMetrics.rmse(computedValues(i), expectedResult(i)) should be < maxRMSE
      }
    }
  }
}
