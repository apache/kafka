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

import kafka.metrics.KafkaMetricsGroup
import java.util.concurrent.TimeUnit
import java.util.Random
import scala.math._

/**
 * A class to measure and throttle the rate of some process. The throttler takes a desired rate-per-second
 * (the units of the process don't matter, it could be bytes or a count of some other thing), and will sleep for 
 * an appropriate amount of time when maybeThrottle() is called to attain the desired rate.
 * 
 * @param desiredRatePerSec: The rate we want to hit in units/sec
 * @param checkIntervalMs: The interval at which to check our rate
 * @param throttleDown: Does throttling increase or decrease our rate?
 * @param time: The time implementation to use
 */
@threadsafe
class Throttler(val desiredRatePerSec: Double, 
                val checkIntervalMs: Long = 100L, 
                val throttleDown: Boolean = true,
                metricName: String = "throttler",
                units: String = "entries",
                val time: Time = SystemTime) extends Logging with KafkaMetricsGroup {
  
  private val lock = new Object
  private val meter = newMeter(metricName, units, TimeUnit.SECONDS)
  private var periodStartNs: Long = time.nanoseconds
  private var observedSoFar: Double = 0.0
  
  def maybeThrottle(observed: Double) {
    meter.mark(observed.toLong)
    lock synchronized {
      observedSoFar += observed
      val now = time.nanoseconds
      val elapsedNs = now - periodStartNs
      // if we have completed an interval AND we have observed something, maybe
      // we should take a little nap
      if(elapsedNs > checkIntervalMs * Time.NsPerMs && observedSoFar > 0) {
        val rateInSecs = (observedSoFar * Time.NsPerSec) / elapsedNs
        val needAdjustment = !(throttleDown ^ (rateInSecs > desiredRatePerSec))
        if(needAdjustment) {
          // solve for the amount of time to sleep to make us hit the desired rate
          val desiredRateMs = desiredRatePerSec / Time.MsPerSec.toDouble
          val elapsedMs = elapsedNs / Time.NsPerMs
          val sleepTime = round(observedSoFar / desiredRateMs - elapsedMs)
          if(sleepTime > 0) {
            trace("Natural rate is %f per second but desired rate is %f, sleeping for %d ms to compensate.".format(rateInSecs, desiredRatePerSec, sleepTime))
            time.sleep(sleepTime)
          }
        }
        periodStartNs = now
        observedSoFar = 0
      }
    }
  }
  
}

object Throttler {
  
  def main(args: Array[String]) {
    val rand = new Random()
    val throttler = new Throttler(100000, 100, true, time = SystemTime)
    val interval = 30000
    var start = System.currentTimeMillis
    var total = 0
    while(true) {
      val value = rand.nextInt(1000)
      Thread.sleep(1)
      throttler.maybeThrottle(value)
      total += value
      val now = System.currentTimeMillis
      if(now - start >= interval) {
        println(total / (interval/1000.0))
        start = now
        total = 0
      }
    }
  }
}
