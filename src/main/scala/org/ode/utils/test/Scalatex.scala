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

import java.io.{File,PrintWriter}
import scala.collection.mutable.Stack


/**
  * Latex document generator class, aka ScaLaTex
  * Author: Alexandre Degurse
  */


class Scalatex(
  path: String,
  documentClass: String = "report",
  title: String = "ScaLaTex Document",
  author: String = "Project-ODE",
  date: String = "\\today",
  val indent: Boolean = true,
  extraPackages: List[String] = null
  ) {

  val outFile = new File(path)
  val outPrinter = new PrintWriter(outFile , "UTF-8")

  val environmentStack = Stack[String]()

  // write document essential elements

  write(s"\\documentclass{${documentClass}}\n")

  write("\\usepackage[english]{babel}")
  write("\\usepackage[utf8]{inputenc}")
  write("\\usepackage{array}")

  // add extra latex packages if any given
  if (extraPackages != null) {
    for (extraPackage <- extraPackages) {
      write(s"\\usepackage{${extraPackage}}")
    }
  }

  write("")

  write(s"\\title{${title}}")
  write(s"\\date{${date}}")
  write(s"\\author{${author}}")

  write("")

  write("\\begin{document}")
  write("\\maketitle")

  write("")

  environmentStack.push("document")

  def section(name: String) {
    newEnvironment(s"\\section{${name}}", "section")
  }

  def subsection(name: String) {
    newEnvironment(s"\\subsection{${name}}", "subsection")
  }

  def addPlainText(text: String) {
    write(text + "\n")
  }

  def array(layout: String) {
    newEnvironment(s"\\begin{tabular}{${layout}}","tabular")
  }

  def addArrayItem(items: Array[String]) {
    if (environmentStack.head != "tabular") {
      throw new IllegalAccessException("Current environment is not an array !")
    }

    val row: String = items.reduce((p,n) => p + " & " + n) 

    write(row + "\\\\")
  }

  def addArrayItemFromDoubles(items: Array[Double]) {
    if (environmentStack.head != "tabular") {
      throw new IllegalAccessException("Current environment is not an array !")
    }

    val row: String = items
      .map(value => f"$value%,.3f")
      .reduce((p,n) => p + " & " + n) 

    write(row + "\\\\")
  }

  def addArrayHline(){
    write("\\hline")
  } 

  def write(element: String) {
    if (indent){
      outPrinter.println("    " * environmentStack.length + element)
    } else {
      outPrinter.println(element)
    }

  }

  def end() {
    closeAllEnvironments()
    outPrinter.close()
  }

  /**
   * Environment functions used to manage LaTex environments
   *
   * Having all environments in the stack (and not just the ones that need closure) enables 
   * the class to indent properly
   */

  def newEnvironment(toWrite:String, environment: String) {
    closeEnvironmentIfNeeded(environment)
    write(toWrite)
    write("")
    environmentStack.push(environment)
  }

  def environmentLevel(environment: String): Int = environment match {
    case "document" => 0
    case "part" => 1
    case "section" => 2
    case "subsection" => 3
    case _ => 4
  }

  def environmentNeedsClosure(environment: String): Boolean = environment match {
    case "document" => true
    case "tabular" => true
    case _ => false
  }

  def closeEnvironmentIfNeeded(newEnvironment: String) {
    while (compareEnvironment(newEnvironment)) {
      closeEnvironment()
    }
  }

  /**
   * Function used to compare environment, if the given environment is a environment of higher or equal
   * level than the first in the environmentStack, then the one in the stack needs to 
   * be ended, a closure might be needed (eg \end{array}).
   */
  def compareEnvironment(environment: String): Boolean = {
    return (environmentLevel(environmentStack.head) >= environmentLevel(environment))
  }

  def closeEnvironment() {
    val env = environmentStack.pop
    if (environmentNeedsClosure(env)) {
      write("")
      write(s"\\end{${env}}")
    }
  }

  def closeAllEnvironments() {
    while (!environmentStack.isEmpty) {
      closeEnvironment()
    }
  }
}
