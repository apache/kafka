/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.controller

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger

import com.yammer.metrics.Metrics
import com.yammer.metrics.core.Timer
import kafka.utils.TestUtils
import org.junit.{After, Test}
import org.junit.Assert.{assertEquals, fail}

import scala.collection.JavaConverters._

class ControllerEventManagerTest {

  private var controllerEventManager: ControllerEventManager = _

  @After
  def tearDown(): Unit = {
    if (controllerEventManager != null)
      controllerEventManager.close()
  }

  @Test
  def testSuccessfulEvent(): Unit = {
    check("kafka.controller:type=ControllerStats,name=AutoLeaderBalanceRateAndTimeMs", ControllerState.AutoLeaderBalance,
      () => Unit)
  }

  @Test
  def testEventThatThrowsException(): Unit = {
    check("kafka.controller:type=ControllerStats,name=LeaderElectionRateAndTimeMs", ControllerState.BrokerChange,
      () => throw new NullPointerException)
  }

  private def check(metricName: String, controllerState: ControllerState, process: () => Unit): Unit = {
    val controllerStats = new ControllerStats
    val eventProcessedListenerCount = new AtomicInteger
    controllerEventManager = new ControllerEventManager(controllerStats.rateAndTimeMetrics,
      _ => eventProcessedListenerCount.incrementAndGet)
    controllerEventManager.start()

    val initialTimerCount = timer(metricName).count

    // Only return from `process()` once we have checked `controllerEventManager.state`
    val latch = new CountDownLatch(1)
    val eventMock = ControllerTestUtils.createMockControllerEvent(controllerState, { () =>
      latch.await()
      process()
    })

    controllerEventManager.put(eventMock)
    TestUtils.waitUntilTrue(() => controllerEventManager.state == controllerState,
      s"Controller state is not $controllerState")
    latch.countDown()

    TestUtils.waitUntilTrue(() => controllerEventManager.state == ControllerState.Idle,
      "Controller state has not changed back to Idle")
    assertEquals(1, eventProcessedListenerCount.get)

    assertEquals("Timer has not been updated", initialTimerCount + 1, timer(metricName).count)
  }

  private def timer(metricName: String): Timer = {
    Metrics.defaultRegistry.allMetrics.asScala.filterKeys(_.getMBeanName == metricName).values.headOption
      .getOrElse(fail(s"Unable to find metric $metricName")).asInstanceOf[Timer]
  }

}
