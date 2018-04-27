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

import kafka.network.RequestChannel.{EndThrottlingAction, ResponseAction, StartThrottlingAction}
import org.apache.kafka.common.metrics.MetricConfig
import org.apache.kafka.common.utils.MockTime
import org.junit.{Assert, Before, Test}

class ThrottledChannelExpirationTest {
  private val time = new MockTime
  private var numCallbacksForStartThrottling: Int = 0
  private var numCallbacksForEndThrottling: Int = 0
  private val metrics = new org.apache.kafka.common.metrics.Metrics(new MetricConfig(),
                                                                    Collections.emptyList(),
                                                                    time)

  def callback(responseAction: ResponseAction): Unit = {
    responseAction match {
      case StartThrottlingAction => numCallbacksForStartThrottling += 1
      case EndThrottlingAction => numCallbacksForEndThrottling += 1
    }
  }

  @Before
  def beforeMethod() {
    numCallbacksForStartThrottling = 0
    numCallbacksForEndThrottling = 0
  }

  @Test
  def testCallbackInvocationAfterExpiration() {
    val clientMetrics = new ClientQuotaManager(ClientQuotaManagerConfig(), metrics, QuotaType.Produce, time, "")

    val delayQueue = new DelayQueue[ThrottledChannel]()
    val reaper = new clientMetrics.ThrottledChannelReaper(delayQueue, "")
    try {
      // Add 4 elements to the queue out of order. Add 2 elements with the same expire timestamp.
      val channel1 = new ThrottledChannel(time, 10, callback)
      val channel2 = new ThrottledChannel(time, 30, callback)
      val channel3 = new ThrottledChannel(time, 30, callback)
      val channel4 = new ThrottledChannel(time, 20, callback)
      delayQueue.add(channel1)
      delayQueue.add(channel2)
      delayQueue.add(channel3)
      delayQueue.add(channel4)
      Assert.assertEquals(4, numCallbacksForStartThrottling)

      for(itr <- 1 to 3) {
        time.sleep(10)
        reaper.doWork()
        Assert.assertEquals(itr, numCallbacksForEndThrottling)
      }
      reaper.doWork()
      Assert.assertEquals(4, numCallbacksForEndThrottling)
      Assert.assertEquals(0, delayQueue.size())
      reaper.doWork()
      Assert.assertEquals(4, numCallbacksForEndThrottling)
    } finally {
      clientMetrics.shutdown()
    }
  }

  @Test
  def testThrottledChannelDelay() {
    val t1: ThrottledChannel = new ThrottledChannel(time, 10, callback)
    val t2: ThrottledChannel = new ThrottledChannel(time, 20, callback)
    val t3: ThrottledChannel = new ThrottledChannel(time, 20, callback)
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