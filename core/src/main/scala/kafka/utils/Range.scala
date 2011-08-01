/*
 * Copyright 2010 LinkedIn
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.utils

import scala.math._

/** 
 * A generic range value with a start and end 
 */
trait Range {
  /** The first index in the range */
  def start: Long
  /** The total number of indexes in the range */
  def size: Long
  /** Return true iff the range is empty */
  def isEmpty: Boolean = size == 0

  /** if value is in range */
  def contains(value: Long): Boolean = {
    if( (size == 0 && value == start) ||
        (size > 0 && value >= start && value <= start + size - 1) )
      return true
    else
      return false
  }
  
  override def toString() = "(start=" + start + ", size=" + size + ")"
}
