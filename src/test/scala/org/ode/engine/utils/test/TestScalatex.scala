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

package org.ode.utils.test

import org.scalatest.{FlatSpec, Matchers}

import scala.io.Source
import java.io.{File, FileInputStream, InputStream}

/**
  * Tests for Scalatex class
  * Author: Alexandre Degurse
  */


class TestScalatex extends FlatSpec with Matchers {

  "Scalatex" should "generate a empty Latex document" in {
    val slt = new Scalatex("/tmp/empty.tex")
    slt.end()

    val outFile: File = new File("/tmp/empty.tex")
    val outFileInputStream: InputStream = new FileInputStream(outFile)
    val outString: String = Source.fromInputStream(outFileInputStream).mkString

    val expFile: File = new File("src/test/resources/tex/empty.tex")
    val expFileInputStream: InputStream = new FileInputStream(expFile)
    val expectedString: String = Source.fromInputStream(expFileInputStream).mkString
    
    outString should be(expectedString)
  }

  it should "generate a sample LaTex document with indents" in {

    val slt = new Scalatex("/tmp/example-without-indent.tex")

    slt.section("First section")
    slt.section("Second section")
    slt.subsection("First subsection")
    slt.addPlainText("Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod")
    slt.subsection("Second subsection")
    slt.array("|l|cc")
    slt.addArrayItem(Array("elem at 0,0","elem at 0,1","elem at 0,2"))
    slt.addArrayHline()
    slt.addArrayItemFromDoubles((1.0 to 3.0 by 1.0).toArray)
    slt.end()

    val outFile: File = new File("/tmp/example-without-indent.tex")
    val outFileInputStream: InputStream = new FileInputStream(outFile)
    val outString: String = Source.fromInputStream(outFileInputStream).mkString

    val expFile: File = new File("src/test/resources/tex/example-without-indent.tex")
    val expFileInputStream: InputStream = new FileInputStream(expFile)
    val expectedString: String = Source.fromInputStream(expFileInputStream).mkString

    outString should be(expectedString)
  }

  it should "generate a sample LaTex document without indents" in {

    val slt = new Scalatex("/tmp/example.tex", indent=false)

    slt.section("First section")
    slt.section("Second section")
    slt.subsection("First subsection")
    slt.addPlainText("Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod")
    slt.subsection("Second subsection")
    slt.array("|l|cc")
    slt.addArrayItem(Array("elem at 0,0","elem at 0,1","elem at 0,2"))
    slt.addArrayHline()
    slt.addArrayItemFromDoubles((1.0 to 3.0 by 1.0).toArray)
    slt.end()

    val outFile: File = new File("/tmp/example.tex")
    val outFileInputStream: InputStream = new FileInputStream(outFile)
    val outString: String = Source.fromInputStream(outFileInputStream).mkString

    val expFile: File = new File("src/test/resources/tex/example.tex")
    val expFileInputStream: InputStream = new FileInputStream(expFile)
    val expectedString: String = Source.fromInputStream(expFileInputStream).mkString

    outString should be(expectedString)
  }

  it should "fail on insert of a new table when no table were declared" in {
    val slt = new Scalatex("/tmp/fail.tex")

    an [IllegalAccessException] should be thrownBy slt.addArrayItem(Array("test"))
  }
}