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

package org.ode.engine.standardization


import org.scalatest.{FlatSpec, Matchers}


/**
 * Some tests for ResultsHandler class
 */

class TestResultsHandler extends FlatSpec with Matchers {

  it should "read a result file when given its parameters" in {
    val sp = SoundHandler("Sound2", 64, 24, 3120, 2000.0, 1)
    val refValuesLocation = "/standardization/scipy/values"


    val rp = ResultsHandler(sp, "vTOL", 2000, 2000, 2000, 64, refValuesLocation)

    val expectedParametersString = "Sound2_64_24_3120_2000.0_1_vTOL_2000_2000_2000_64"
    val expectedName = "Sound2_64_24_3120_2000.0_1_vTOL_2000_2000_2000_64.csv"

    rp.fileName should equal(expectedName)
    rp.paramsString should equal(expectedParametersString)

    val results = rp.getExpectedValues()

    val expectedNumberOfFeature = 1
    val expectedFeatureLength = 4

    results.length should equal(expectedNumberOfFeature)
    results(0).length should equal(expectedFeatureLength)

  }
}
