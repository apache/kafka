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

import com.codahale.metrics.{Histogram, Timer}
import kafka.utils.TestUtils
import org.apache.kafka.common.utils.MockTime
import org.junit.Assert.{assertEquals, assertTrue, fail}
import org.junit.{After, Test}

class ControllerEventManagerTest {

  private var controllerEventManager: ControllerEventManager = _

  @After
  def tearDown(): Unit = {
    if (controllerEventManager != null)
      controllerEventManager.close()
  }

  @Test
  def testMetricsCleanedOnClose(): Unit = {
    val time = new MockTime()
    val controllerStats = new ControllerStats
    val eventProcessor = new ControllerEventProcessor {
      override def process(event: ControllerEvent): Unit = {}
      override def preempt(event: ControllerEvent): Unit = {}
    }

    def allEventManagerMetrics: Set[String] = {
      kafka.metrics.getKafkaMetrics.keySet
        .filter(_.startsWith("kafka.controller:type=ControllerEventManager"))
    }

    controllerEventManager = new ControllerEventManager(0, eventProcessor,
      time, controllerStats.rateAndTimeMetrics)
    controllerEventManager.start()
    assertTrue(allEventManagerMetrics.nonEmpty)

    controllerEventManager.close()
    assertTrue(allEventManagerMetrics.isEmpty)
  }

  @Test
  def testEventQueueTime(): Unit = {
    val metricName = "kafka.controller:type=ControllerEventManager,name=EventQueueTimeMs"
    val controllerStats = new ControllerStats
    val time = new MockTime()
    val latch = new CountDownLatch(1)

    val eventProcessor = new ControllerEventProcessor {
      override def process(event: ControllerEvent): Unit = {
        latch.await()
        time.sleep(500)
      }
      override def preempt(event: ControllerEvent): Unit = {}
    }

    // The metric should not already exist
    assertTrue(kafka.metrics.getKafkaMetrics.filterKeys(_ == metricName).values.isEmpty)

    controllerEventManager = new ControllerEventManager(0, eventProcessor,
      time, controllerStats.rateAndTimeMetrics)
    controllerEventManager.start()

    controllerEventManager.put(TopicChange)
    controllerEventManager.put(TopicChange)
    latch.countDown()

    val queueTimeHistogram = kafka.metrics.getKafkaMetrics.filterKeys(_ == metricName).values.headOption
      .getOrElse(fail(s"Unable to find metric $metricName")).asInstanceOf[Histogram]

    TestUtils.waitUntilTrue(() => controllerEventManager.isEmpty,
      "Timed out waiting for processing of all events")

    assertEquals(2, queueTimeHistogram.getCount)
    assertEquals(0, queueTimeHistogram.getSnapshot.getMin, 0.01)
    assertEquals(500, queueTimeHistogram.getSnapshot.getMax, 0.01)
  }

  @Test
  def testSuccessfulEvent(): Unit = {
    check("kafka.controller:type=ControllerStats,name=AutoLeaderBalanceRateAndTime",
      AutoPreferredReplicaLeaderElection, () => ())
  }

  @Test
  def testEventThatThrowsException(): Unit = {
    check("kafka.controller:type=ControllerStats,name=LeaderElectionRateAndTime",
      BrokerChange, () => throw new NullPointerException)
  }

  private def check(metricName: String,
                    event: ControllerEvent,
                    func: () => Unit): Unit = {
    val controllerStats = new ControllerStats
    val eventProcessedListenerCount = new AtomicInteger
    val latch = new CountDownLatch(1)
    val eventProcessor = new ControllerEventProcessor {
      override def process(event: ControllerEvent): Unit = {
        // Only return from `process()` once we have checked `controllerEventManager.state`
        latch.await()
        eventProcessedListenerCount.incrementAndGet()
        func()
      }
      override def preempt(event: ControllerEvent): Unit = {}
    }

    controllerEventManager = new ControllerEventManager(0, eventProcessor,
      new MockTime(), controllerStats.rateAndTimeMetrics)
    controllerEventManager.start()

    val initialTimerCount = timer(metricName).getCount

    controllerEventManager.put(event)
    TestUtils.waitUntilTrue(() => controllerEventManager.state == event.state,
      s"Controller state is not ${event.state}")
    latch.countDown()

    TestUtils.waitUntilTrue(() => controllerEventManager.state == ControllerState.Idle,
      "Controller state has not changed back to Idle")
    assertEquals(1, eventProcessedListenerCount.get)

    assertEquals("Timer has not been updated", initialTimerCount + 1, timer(metricName).getCount)
  }

  private def timer(metricName: String): Timer = {
    kafka.metrics.getKafkaMetrics.filterKeys(_ == metricName).values.headOption
      .getOrElse(fail(s"Unable to find metric $metricName")).asInstanceOf[Timer]
  }

}
