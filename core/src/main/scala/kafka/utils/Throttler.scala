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

package kafka.utils;

import scala.math._

object Throttler extends Logging {
  val DefaultCheckIntervalMs = 100L
}

/**
 * A class to measure and throttle the rate of some process. The throttler takes a desired rate-per-second
 * (the units of the process don't matter, it could be bytes or a count of some other thing), and will sleep for 
 * an appropraite amount of time when maybeThrottle() is called to attain the desired rate.
 * 
 * @param desiredRatePerSec: The rate we want to hit in units/sec
 * @param checkIntervalMs: The interval at which to check our rate
 * @param throttleDown: Does throttling increase or decrease our rate?
 * @param time: The time implementation to use
 */
@nonthreadsafe
class Throttler(val desiredRatePerSec: Double, 
                val checkIntervalMs: Long, 
                val throttleDown: Boolean, 
                val time: Time) {
  
  private val lock = new Object
  private var periodStartNs: Long = time.nanoseconds
  private var observedSoFar: Double = 0.0
  
  def this(desiredRatePerSec: Double, throttleDown: Boolean) = 
    this(desiredRatePerSec, Throttler.DefaultCheckIntervalMs, throttleDown, SystemTime)

  def this(desiredRatePerSec: Double) = 
    this(desiredRatePerSec, Throttler.DefaultCheckIntervalMs, true, SystemTime)
  
  def maybeThrottle(observed: Double) {
    lock synchronized {
      observedSoFar += observed
      val now = time.nanoseconds
      val ellapsedNs = now - periodStartNs
      // if we have completed an interval AND we have observed something, maybe
      // we should take a little nap
      if(ellapsedNs > checkIntervalMs * Time.NsPerMs && observedSoFar > 0) {
        val rateInSecs = (observedSoFar * Time.NsPerSec) / ellapsedNs
        val needAdjustment = !(throttleDown ^ (rateInSecs > desiredRatePerSec))
        if(needAdjustment) {
          // solve for the amount of time to sleep to make us hit the desired rate
          val desiredRateMs = desiredRatePerSec / Time.MsPerSec.asInstanceOf[Double]
          val ellapsedMs = ellapsedNs / Time.NsPerMs
          val sleepTime = round(observedSoFar / desiredRateMs - ellapsedMs)
          if(sleepTime > 0) {
            Throttler.debug("Natural rate is " + rateInSecs + " per second but desired rate is " + desiredRatePerSec + 
                                     ", sleeping for " + sleepTime + " ms to compensate.")
            time.sleep(sleepTime)
          }
        }
        periodStartNs = now
        observedSoFar = 0
      }
    }
  }
  
}
