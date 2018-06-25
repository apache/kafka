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

import kafka.network
import kafka.network.RequestChannel
import kafka.network.RequestChannel.Response
import kafka.utils.Logging
import org.apache.kafka.common.utils.Time


/**
  * Represents a request whose response has been delayed.
  * @param request The request that has been delayed
  * @param time Time instance to use
  * @param throttleTimeMs Delay associated with this request
  * @param channelThrottlingCallback Callback for channel throttling
  */
class ThrottledChannel(val request: RequestChannel.Request, val time: Time, val throttleTimeMs: Int, channelThrottlingCallback: Response => Unit)
  extends Delayed with Logging {
  var endTime = time.milliseconds + throttleTimeMs

  // Notify the socket server that throttling has started for this channel.
  channelThrottlingCallback(new RequestChannel.StartThrottlingResponse(request))

  // Notify the socket server that throttling has been done for this channel.
  def notifyThrottlingDone(): Unit = {
    trace("Channel throttled for: " + throttleTimeMs + " ms")
    channelThrottlingCallback(new network.RequestChannel.EndThrottlingResponse(request))
  }

  override def getDelay(unit: TimeUnit): Long = {
    unit.convert(endTime - time.milliseconds, TimeUnit.MILLISECONDS)
  }

  override def compareTo(d: Delayed): Int = {
    val other = d.asInstanceOf[ThrottledChannel]
    if (this.endTime < other.endTime) -1
    else if (this.endTime > other.endTime) 1
    else 0
  }
}