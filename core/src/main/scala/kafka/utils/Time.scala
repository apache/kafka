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

/**
 * Some common constants
 */
object Time {
  val NsPerUs = 1000
  val UsPerMs = 1000
  val MsPerSec = 1000
  val NsPerMs = NsPerUs * UsPerMs
  val NsPerSec = NsPerMs * MsPerSec
  val UsPerSec = UsPerMs * MsPerSec
  val SecsPerMin = 60
  val MinsPerHour = 60
  val HoursPerDay = 24
  val SecsPerHour = SecsPerMin * MinsPerHour
  val SecsPerDay = SecsPerHour * HoursPerDay
  val MinsPerDay = MinsPerHour * HoursPerDay
}

/**
 * A mockable interface for time functions
 */
trait Time {
  
  def absoluteMilliseconds: Long

  def relativeMilliseconds: Long

  def relativeNanoseconds: Long

  def sleep(ms: Long)
}

/**
 * The normal system implementation of time functions
 */
object SystemTime extends Time {

  /**
    * Returns the number of milliseconds since midnight UTC on 1/1/1970 based on the operating system's clock.
    *
    * This value is likely less precise than relative time.
    */
  def absoluteMilliseconds: Long = System.currentTimeMillis

  /**
    * Returns the current value of the JVM's high resolution time source rounded down to milliseconds.
    *
    * This time value can only be used for measuring elapsed time within the JVM from which it is generated.
    * This time value has no relation to the "wall-clock" time.
    */
  def relativeMilliseconds: Long = relativeNanoseconds / Time.NsPerMs

  /**
    * Returns the current value of the JVM's high resolution time source in nanoseconds.
    *
    * This time value can only be used for measuring elapsed time within the JVM from which it is generated.
    * This time value has no relation to the "wall-clock" time.
    */
  def relativeNanoseconds: Long = System.nanoTime
  
  def sleep(ms: Long): Unit = Thread.sleep(ms)
  
}
