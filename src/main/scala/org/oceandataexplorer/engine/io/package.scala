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

package org.oceandataexplorer.engine

/**
 * Window Functions package object providing types for window functions.
 */
package object io {

  /**
   * Enumeration providing actions for handling a incomplete record
   */
  object LastRecordAction {

    /**
     * The trait for the last record action types
     * The available types for last record action:
     */
    sealed trait LastRecordAction

    /**
     * - Skip: any incomplete record will be dropped
     */
    case object Skip extends LastRecordAction {
      /**
       * Method allowing to get the string that corresponds to the
       * LastRecordAction for hadoop-io-extensions
       * @return The corresponding LastRecordAction as a String
       */
      override def toString: String = "skip"
    }

    /**
     * - Fail: if a incomplete record is encountered, the record reader fails
     */
    case object Fail extends LastRecordAction {
      /**
       * Method allowing to get the string that corresponds to the
       * LastRecordAction for hadoop-io-extensions
       * @return The corresponding LastRecordAction as a String
       */
      override def toString: String = "fail"
    }

    /**
     * - Fill: any incomplete record will be filled with zeros
     */
    case object Fill extends LastRecordAction {
      /**
       * Method allowing to get the string that corresponds to the
       * LastRecordAction for hadoop-io-extensions
       * @return The corresponding LastRecordAction as a String
       */
      override def toString: String = "fill"
    }
  }
}

