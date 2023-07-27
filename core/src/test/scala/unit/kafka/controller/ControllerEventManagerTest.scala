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

import com.yammer.metrics.core.{Histogram, MetricName, Timer}
import kafka.controller
import kafka.utils.TestUtils
import org.apache.kafka.common.message.UpdateMetadataResponseData
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.UpdateMetadataResponse
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.server.metrics.KafkaYammerMetrics
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue, fail}
import org.junit.jupiter.api.{AfterEach, Test}

import scala.jdk.CollectionConverters._
import scala.collection.mutable

class ControllerEventManagerTest {

  private var controllerEventManager: ControllerEventManager = _

  @AfterEach
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

    def allEventManagerMetrics: Set[MetricName] = {
      KafkaYammerMetrics.defaultRegistry.allMetrics.asScala.keySet
        .filter(_.getMBeanName.startsWith("kafka.controller:type=ControllerEventManager"))
        .toSet
    }

    controllerEventManager = new ControllerEventManager(0, eventProcessor,
      time, controllerStats.rateAndTimeMetrics)
    controllerEventManager.start()
    assertTrue(allEventManagerMetrics.nonEmpty)

    controllerEventManager.close()
    assertTrue(allEventManagerMetrics.isEmpty)
  }

  @Test
  def testEventWithoutRateMetrics(): Unit = {
    val time = new MockTime()
    val controllerStats = new ControllerStats
    val processedEvents = mutable.Set.empty[ControllerEvent]

    val eventProcessor = new ControllerEventProcessor {
      override def process(event: ControllerEvent): Unit = { processedEvents += event }
      override def preempt(event: ControllerEvent): Unit = {}
    }

    controllerEventManager = new ControllerEventManager(0, eventProcessor,
      time, controllerStats.rateAndTimeMetrics)
    controllerEventManager.start()

    val updateMetadataResponse = new UpdateMetadataResponse(
      new UpdateMetadataResponseData().setErrorCode(Errors.NONE.code)
    )
    val updateMetadataResponseEvent = controller.UpdateMetadataResponseReceived(updateMetadataResponse, brokerId = 1)
    controllerEventManager.put(updateMetadataResponseEvent)
    TestUtils.waitUntilTrue(() => processedEvents.size == 1,
      "Failed to process expected event before timing out")
    assertEquals(updateMetadataResponseEvent, processedEvents.head)
  }

  @Test
  def testEventQueueTime(): Unit = {
    val metricName = "kafka.controller:type=ControllerEventManager,name=EventQueueTimeMs"
    val controllerStats = new ControllerStats
    val time = new MockTime()
    val latch = new CountDownLatch(1)
    val processedEvents = new AtomicInteger()

    val eventProcessor = new ControllerEventProcessor {
      override def process(event: ControllerEvent): Unit = {
        latch.await()
        time.sleep(500)
        processedEvents.incrementAndGet()
      }
      override def preempt(event: ControllerEvent): Unit = {}
    }

    // The metric should not already exist
    assertTrue(KafkaYammerMetrics.defaultRegistry.allMetrics.asScala.filter { case (k, _) =>
      k.getMBeanName == metricName
    }.values.isEmpty)

    controllerEventManager = new ControllerEventManager(0, eventProcessor,
      time, controllerStats.rateAndTimeMetrics)
    controllerEventManager.start()

    controllerEventManager.put(TopicChange)
    controllerEventManager.put(TopicChange)
    latch.countDown()

    TestUtils.waitUntilTrue(() => processedEvents.get() == 2,
      "Timed out waiting for processing of all events")

    val queueTimeHistogram = KafkaYammerMetrics.defaultRegistry.allMetrics.asScala.filter { case (k, _) =>
      k.getMBeanName == metricName
    }.values.headOption.getOrElse(fail(s"Unable to find metric $metricName")).asInstanceOf[Histogram]

    assertEquals(2, queueTimeHistogram.count)
    assertEquals(0, queueTimeHistogram.min, 0.01)
    assertEquals(500, queueTimeHistogram.max, 0.01)
  }

  @Test
  def testEventQueueTimeResetOnTimeout(): Unit = {
    val metricName = "kafka.controller:type=ControllerEventManager,name=EventQueueTimeMs"
    val controllerStats = new ControllerStats
    val time = new MockTime()
    val processedEvents = new AtomicInteger()

    val eventProcessor = new ControllerEventProcessor {
      override def process(event: ControllerEvent): Unit = {
        processedEvents.incrementAndGet()
      }
      override def preempt(event: ControllerEvent): Unit = {}
    }

    controllerEventManager = new ControllerEventManager(0, eventProcessor,
      time, controllerStats.rateAndTimeMetrics, 1)
    controllerEventManager.start()

    controllerEventManager.put(TopicChange)
    controllerEventManager.put(TopicChange)

    TestUtils.waitUntilTrue(() => processedEvents.get() == 2,
      "Timed out waiting for processing of all events")

    val queueTimeHistogram = KafkaYammerMetrics.defaultRegistry.allMetrics.asScala.filter { case (k, _) =>
      k.getMBeanName == metricName
    }.values.headOption.getOrElse(fail(s"Unable to find metric $metricName")).asInstanceOf[Histogram]

    TestUtils.waitUntilTrue(() => queueTimeHistogram.count == 0,
      "Timed out on resetting queueTimeHistogram")
    assertEquals(0, queueTimeHistogram.min, 0.1)
    assertEquals(0, queueTimeHistogram.max, 0.1)
  }

  @Test
  def testSuccessfulEvent(): Unit = {
    check("kafka.controller:type=ControllerStats,name=AutoLeaderBalanceRateAndTimeMs",
      AutoPreferredReplicaLeaderElection, () => ())
  }

  @Test
  def testEventThatThrowsException(): Unit = {
    check("kafka.controller:type=ControllerStats,name=LeaderElectionRateAndTimeMs",
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

    val initialTimerCount = timer(metricName).count

    controllerEventManager.put(event)
    TestUtils.waitUntilTrue(() => controllerEventManager.state == event.state,
      s"Controller state is not ${event.state}")
    latch.countDown()

    TestUtils.waitUntilTrue(() => controllerEventManager.state == ControllerState.Idle,
      "Controller state has not changed back to Idle")
    assertEquals(1, eventProcessedListenerCount.get)

    assertEquals(initialTimerCount + 1, timer(metricName).count, "Timer has not been updated")
  }

  private def timer(metricName: String): Timer = {
    KafkaYammerMetrics.defaultRegistry.allMetrics.asScala.filter { case (k, _) =>
      k.getMBeanName == metricName
    }.values.headOption.getOrElse(fail(s"Unable to find metric $metricName")).asInstanceOf[Timer]
  }

}
