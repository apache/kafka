/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.util.concurrent._
import scala.math._

class DelayedItem(delayMs: Long) extends Delayed with Logging {

  private val dueMs = SystemTime.milliseconds + delayMs

  def this(delay: Long, unit: TimeUnit) = this(unit.toMillis(delay))

  /**
   * The remaining delay time
   */
  def getDelay(unit: TimeUnit): Long = {
    unit.convert(max(dueMs - SystemTime.milliseconds, 0), TimeUnit.MILLISECONDS)
  }

  def compareTo(d: Delayed): Int = {
    val other = d.asInstanceOf[DelayedItem]

    if(dueMs < other.dueMs) -1
    else if(dueMs > other.dueMs) 1
    else 0
  }

}
