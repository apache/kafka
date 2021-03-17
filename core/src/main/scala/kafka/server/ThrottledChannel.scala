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

package kafka.server

import java.util.concurrent.{Delayed, TimeUnit}

import kafka.utils.Logging
import org.apache.kafka.common.utils.Time

trait ThrottleCallback {
  def startThrottling(): Unit
  def endThrottling(): Unit
}

/**
  * Represents a request whose response has been delayed.
  * @param time Time instance to use
  * @param throttleTimeMs Delay associated with this request
  * @param callback Callback for channel throttling
  */
class ThrottledChannel(
  val time: Time,
  val throttleTimeMs: Int,
  val callback: ThrottleCallback
) extends Delayed with Logging {

  private val endTimeNanos = time.nanoseconds() + TimeUnit.MILLISECONDS.toNanos(throttleTimeMs)

  // Notify the socket server that throttling has started for this channel.
  callback.startThrottling()

  // Notify the socket server that throttling has been done for this channel.
  def notifyThrottlingDone(): Unit = {
    trace(s"Channel throttled for: $throttleTimeMs ms")
    callback.endThrottling()
  }

  override def getDelay(unit: TimeUnit): Long = {
    unit.convert(endTimeNanos - time.nanoseconds(), TimeUnit.NANOSECONDS)
  }

  override def compareTo(d: Delayed): Int = {
    val other = d.asInstanceOf[ThrottledChannel]
    java.lang.Long.compare(this.endTimeNanos, other.endTimeNanos)
  }
}
