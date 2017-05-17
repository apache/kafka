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


import java.util.Collections
import java.util.concurrent.{DelayQueue, TimeUnit}

import org.apache.kafka.common.metrics.MetricConfig
import org.apache.kafka.common.utils.MockTime
import org.junit.{Assert, Before, Test}

class ThrottledResponseExpirationTest {
  private val time = new MockTime
  private var numCallbacks: Int = 0
  private val metrics = new org.apache.kafka.common.metrics.Metrics(new MetricConfig(),
                                                                    Collections.emptyList(),
                                                                    time)

  def callback(delayTimeMs: Int) {
    numCallbacks += 1
  }

  @Before
  def beforeMethod() {
    numCallbacks = 0
  }

  @Test
  def testExpire() {
    val clientMetrics = new ClientQuotaManager(ClientQuotaManagerConfig(), metrics, QuotaType.Produce, time)

    val delayQueue = new DelayQueue[ThrottledResponse]()
    val reaper = new clientMetrics.ThrottledRequestReaper(delayQueue)
    try {
      // Add 4 elements to the queue out of order. Add 2 elements with the same expire timestamp
      delayQueue.add(new ThrottledResponse(time, 10, callback))
      delayQueue.add(new ThrottledResponse(time, 30, callback))
      delayQueue.add(new ThrottledResponse(time, 30, callback))
      delayQueue.add(new ThrottledResponse(time, 20, callback))

      for(itr <- 1 to 3) {
        time.sleep(10)
        reaper.doWork()
        Assert.assertEquals(itr, numCallbacks)

      }
      reaper.doWork()
      Assert.assertEquals(4, numCallbacks)
      Assert.assertEquals(0, delayQueue.size())
      reaper.doWork()
      Assert.assertEquals(4, numCallbacks)
    } finally {
      clientMetrics.shutdown()
    }
  }

  @Test
  def testThrottledRequest() {
    val t1: ThrottledResponse = new ThrottledResponse(time, 10, callback)
    val t2: ThrottledResponse = new ThrottledResponse(time, 20, callback)
    val t3: ThrottledResponse = new ThrottledResponse(time, 20, callback)
    Assert.assertEquals(10, t1.throttleTimeMs)
    Assert.assertEquals(20, t2.throttleTimeMs)
    Assert.assertEquals(20, t3.throttleTimeMs)

    for(itr <- 0 to 2) {
      Assert.assertEquals(10 - 10*itr, t1.getDelay(TimeUnit.MILLISECONDS))
      Assert.assertEquals(20 - 10*itr, t2.getDelay(TimeUnit.MILLISECONDS))
      Assert.assertEquals(20 - 10*itr, t3.getDelay(TimeUnit.MILLISECONDS))
      time.sleep(10)
    }
  }
}
