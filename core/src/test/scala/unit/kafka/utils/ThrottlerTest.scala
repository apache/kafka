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

package unit.kafka.utils

import kafka.utils.Throttler
import org.apache.kafka.common.utils.MockTime
import org.junit.Test
import org.junit.Assert.assertTrue


class ThrottlerTest {
  @Test
  def testThrottleDesiredRate() {
    val throttleCheckIntervalMs = 100
    val desiredCountPerMs = 1.0
    val desiredRatePerSec = desiredCountPerMs * 1000
    val desiredCountPerInterval = desiredCountPerMs * throttleCheckIntervalMs

    val mockTime = new MockTime()
    val throttler = new Throttler(desiredRatePerSec = desiredRatePerSec,
                                  checkIntervalMs = throttleCheckIntervalMs,
                                  time = mockTime)

    val startTime = mockTime.milliseconds()

    // Observe desiredCountPerInterval at t0 = startTime
    throttler.maybeThrottle(desiredCountPerInterval)
    mockTime.sleep(throttleCheckIntervalMs + 1)

    // Observe desiredCountPerInterval at t1 = t0 + throttleCheckIntervalMs + 1,
    // in total observe 2*desiredCountPerInterval, should block until t2 = t0 + 2*throttleCheckIntervalMs
    throttler.maybeThrottle(desiredCountPerInterval)

    // Observe (2*desiredCountPerInterval-desiredCountPerMs) after throttling at t2,
    throttler.maybeThrottle(2 * desiredCountPerInterval - desiredCountPerMs)
    mockTime.sleep(throttleCheckIntervalMs + 1)

    // Observer desiredCountPerMs at t3 = t2 + throttleCheckIntervalMs + 1,
    // in total observe 2*desiredCountPerInterval, should block until t4 = t2 + 2*throttleCheckIntervalMs
    throttler.maybeThrottle(desiredCountPerMs)

    // Expect 4*desiredCountPerInterval observation and 4*throttleCheckIntervalMs elapsedTimeMs
    val elapsedTimeMs = mockTime.milliseconds() - startTime
    val actualRatePerSec = 4 * desiredCountPerInterval * 1000 / elapsedTimeMs
    assertTrue(actualRatePerSec <= desiredRatePerSec)
  }
}
