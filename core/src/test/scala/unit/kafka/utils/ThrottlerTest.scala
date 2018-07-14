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
import org.junit.Assert.{assertTrue, assertEquals}


class ThrottlerTest {
  @Test
  def testThrottleDesiredRate() {
    val throttleCheckIntervalMs = 100
    val desiredCountPerMs = 1.0
    val desiredCountPerSec = desiredCountPerMs * 1000
    val desiredCountPerInterval = desiredCountPerMs * throttleCheckIntervalMs

    val mockTime = new MockTime()
    val throttler = new Throttler(desiredRatePerSec = desiredCountPerSec,
                                  checkIntervalMs = throttleCheckIntervalMs,
                                  time = mockTime)

    val firstThrottlePeriodStartTime = mockTime.milliseconds()

    // Observe desiredCountPerInterval at startTime
    throttler.maybeThrottle(desiredCountPerInterval)
    assertEquals(firstThrottlePeriodStartTime, mockTime.milliseconds())
    mockTime.sleep(throttleCheckIntervalMs + 1)

    // Observe desiredCountPerInterval at startTime + throttleCheckIntervalMs + 1,
    val firstThrottlePeriodEndTime = mockTime.milliseconds()
    throttler.maybeThrottle(desiredCountPerInterval)
    val secondThrottlePeriodStartTime = mockTime.milliseconds()
    assertTrue(s"Observe ${2*desiredCountPerInterval} within ${throttleCheckIntervalMs + 1}ms, " +
      s"should block for ${throttleCheckIntervalMs - 1}ms instead of ${secondThrottlePeriodStartTime - firstThrottlePeriodEndTime}ms.",
      secondThrottlePeriodStartTime >= firstThrottlePeriodStartTime + 2*throttleCheckIntervalMs)

    // Observe (2*desiredCountPerInterval-desiredCountPerMs) at secondThrottlePeriodStartTime
    throttler.maybeThrottle(2 * desiredCountPerInterval - desiredCountPerMs)
    assertEquals(secondThrottlePeriodStartTime, mockTime.milliseconds())
    mockTime.sleep(throttleCheckIntervalMs + 1)

    // Observe desiredCountPerMs at secondThrottlePeriodStartTime + throttleCheckIntervalMs + 1
    val secondThrottlePeriodEndTime = mockTime.milliseconds()
    throttler.maybeThrottle(desiredCountPerMs)
    val now = mockTime.milliseconds()
    assertTrue(s"Observe ${2*desiredCountPerInterval} within ${throttleCheckIntervalMs + 1}ms, " +
      s"should block for ${throttleCheckIntervalMs - 1}ms instead of ${now - secondThrottlePeriodEndTime}ms.",
      now >= secondThrottlePeriodStartTime + 2*throttleCheckIntervalMs)

    val elapsedTimeMs = now - firstThrottlePeriodStartTime
    val actualCountPerSec = 4 * desiredCountPerInterval * 1000 / elapsedTimeMs
    assertTrue(s"Actual rate ${actualCountPerSec} is larger than the desired rate ${desiredCountPerSec}",
      actualCountPerSec <= desiredCountPerSec)
  }
}
