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

package kafka.server.metadata

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger

import com.yammer.metrics.core.{Gauge, Histogram, MetricName}
import kafka.metrics.KafkaYammerMetrics
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.apache.kafka.common.protocol.ApiMessage
import org.apache.kafka.common.utils.MockTime
import org.junit.Assert.{assertEquals, assertTrue, fail}
import org.junit.{After, Before, Test}
import org.mockito.Mockito.mock

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

class BrokerMetadataListenerTest {
  val expectedMetricMBeanPrefix = "kafka.server.metadata:type=BrokerMetadataListener"
  val expectedEventQueueTimeMsMetricName = "EventQueueTimeMs"
  val expectedErrorCountMetricName = "ErrorCount"
  val expectedEventQueueSizeMetricName = "EventQueueSize"
  val errorCountMetricMBeanName = s"$expectedMetricMBeanPrefix,name=$expectedErrorCountMetricName"
  val eventQueueSizeMetricMBeanName = s"$expectedMetricMBeanPrefix,name=$expectedEventQueueSizeMetricName"
  val eventQueueTimeMsMetricMBeanName = s"$expectedMetricMBeanPrefix,name=$expectedEventQueueTimeMsMetricName"

  def errorCountGauge() = KafkaYammerMetrics.defaultRegistry.allMetrics.asScala.filter {
    case (k, _) => k.getMBeanName == errorCountMetricMBeanName
  }.values.headOption.getOrElse(fail(s"Unable to find metric $errorCountMetricMBeanName")).asInstanceOf[Gauge[Int]]
  def eventQueueSizeGauge() = KafkaYammerMetrics.defaultRegistry.allMetrics.asScala.filter {
    case (k, _) => k.getMBeanName == eventQueueSizeMetricMBeanName
  }.values.headOption.getOrElse(fail(s"Unable to find metric $eventQueueSizeMetricMBeanName")).asInstanceOf[Gauge[Int]]
  def queueTimeHistogram() = KafkaYammerMetrics.defaultRegistry.allMetrics.asScala.filter {
    case (k, _) => k.getMBeanName == eventQueueTimeMsMetricMBeanName
  }.values.headOption.getOrElse(fail(s"Unable to find metric $eventQueueTimeMsMetricMBeanName")).asInstanceOf[Histogram]
  def allRegisteredMetricNames: Set[MetricName] = {
    KafkaYammerMetrics.defaultRegistry.allMetrics.asScala.keySet
      .filter(_.getMBeanName.startsWith(expectedMetricMBeanPrefix))
      .toSet
  }

  var brokerMetadataListener: Option[BrokerMetadataListener] = None
  val expectedInitialMetadataOffset = -1

  val processor1Key = "processor1"
  val processor2Key = "processor2"
  val processorKeys = List(processor1Key, processor2Key)
  val metadataLogEventInvocations = mutable.Map[String, ListBuffer[MetadataLogEvent]]()
  val outOfBandRegisterLocalBrokerInvocations = mutable.Map[String, ListBuffer[OutOfBandRegisterLocalBrokerEvent]]()
  val outOfbandFenceLocalBrokerInvocations = mutable.Map[String, ListBuffer[OutOfBandFenceLocalBrokerEvent]]()
  val numStartupEventsProcessed = mutable.Map[String, Int]()
  var numEventsProcessed = 0

  @Before
  def setUp(): Unit = {
    brokerMetadataListener = None
    processorKeys.foreach(key => {
      metadataLogEventInvocations.put(key, ListBuffer.empty)
      outOfBandRegisterLocalBrokerInvocations.put(key, ListBuffer.empty)
      outOfbandFenceLocalBrokerInvocations.put(key, ListBuffer.empty)
      numStartupEventsProcessed.put(key, 0)
      numEventsProcessed = 0
    })
  }

  @After
  def tearDown(): Unit = {
    brokerMetadataListener.foreach(_.close())
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testEmptyBrokerMetadataProcessors(): Unit = {
    new BrokerMetadataListener(mock(classOf[KafkaConfig]), new MockTime(), List.empty)
  }

  @Test
  def testEventIsProcessedAfterStartup(): Unit = {
    val processedEvents = mutable.Buffer.empty[List[ApiMessage]]
    val metadataLogEventIndicator = List(mock(classOf[ApiMessage]))
    val startupEventIndicator = mock(classOf[ApiMessage]) :: metadataLogEventIndicator

    val brokerMetadataProcessor = new BrokerMetadataProcessor {
      override def processStartup(): Unit = {
        processedEvents += startupEventIndicator
      }
      override def process(metadataLogEvent: MetadataLogEvent): Unit = {
        processedEvents += metadataLogEvent.apiMessages
      }
      def process(outOfBandFenceLocalBrokerEvent: OutOfBandFenceLocalBrokerEvent): Unit = {}
      def process(outOfBandRegisterLocalBrokerEvent: OutOfBandRegisterLocalBrokerEvent): Unit = {}
    }

    val listener = start(new BrokerMetadataListener(mock(classOf[KafkaConfig]), new MockTime(), List(brokerMetadataProcessor)))

    listener.put(MetadataLogEvent(metadataLogEventIndicator, 1))
    TestUtils.waitUntilTrue(() => processedEvents.size == 2,
      s"Failed to process expected event before timing out; processed events = ${processedEvents.size}", 5000)
    assertEquals(startupEventIndicator, processedEvents(0))
    assertEquals(metadataLogEventIndicator, processedEvents(1))
  }

  @Test
  def testInitialAndSubsequentMetadataOffsets(): Unit = {
    val listener = start(new BrokerMetadataListener(mock(classOf[KafkaConfig]), new MockTime(),
      twoProcessorsCountingInvocations(processorKeys)))
    assertEquals(expectedInitialMetadataOffset, listener.currentMetadataOffset())

    val nextMetadataOffset = expectedInitialMetadataOffset + 2
    val msg0 = mock(classOf[ApiMessage])
    val msg1 = mock(classOf[ApiMessage])
    val apiMessages = List(msg0, msg1)
    val event = MetadataLogEvent(apiMessages, nextMetadataOffset)
    listener.put(event)

    val expectedNumEventsProcessedPerKey = 2 // 1 successful message + 1 startup
    TestUtils.waitUntilTrue(() => numEventsProcessed == processorKeys.size * expectedNumEventsProcessedPerKey,
      s"Failed to process expected event before timing out; processed events = $numEventsProcessed", 5000)

    // offset should be updated
    assertEquals(nextMetadataOffset, listener.currentMetadataOffset())

    // record should be processed
    processorKeys.foreach(key => {
      assertEquals(1, numStartupEventsProcessed.get(key).get)
      assertEquals(1, metadataLogEventInvocations.get(key).get.size)
      assertEquals(metadataLogEventInvocations.get(key).get.head, event)
      assertEquals(0, outOfBandRegisterLocalBrokerInvocations.get(key).get.size)
      assertEquals(0, outOfbandFenceLocalBrokerInvocations.get(key).get.size)
    })
    // there should be no errors
    assertEquals(0, errorCountGauge().value())
  }

  @Test
  def testOutOfBandHeartbeatMessages(): Unit = {
    val listener = start(new BrokerMetadataListener(mock(classOf[KafkaConfig]), new MockTime(),
      twoProcessorsCountingInvocations(processorKeys)))
    assertEquals(expectedInitialMetadataOffset, listener.currentMetadataOffset())

    val msg0 = OutOfBandRegisterLocalBrokerEvent(1)
    val msg1 = OutOfBandFenceLocalBrokerEvent(1)
    listener.put(msg0)
    listener.put(msg1)

    val expectedNumEventsProcessedPerKey = 3 // 2 successful messages + 1 startup
    TestUtils.waitUntilTrue(() => numEventsProcessed == processorKeys.size * expectedNumEventsProcessedPerKey,
      s"Failed to process expected event before timing out; processed events = $numEventsProcessed", 5000)

    // offset should not be updated
    assertEquals(expectedInitialMetadataOffset, listener.currentMetadataOffset())

    // out-of-band records should be processed
    processorKeys.foreach(key => {
      assertEquals(0, metadataLogEventInvocations.get(key).get.size)
      assertEquals(1, outOfBandRegisterLocalBrokerInvocations.get(key).get.size)
      assertEquals(outOfBandRegisterLocalBrokerInvocations.get(key).get.head, msg0)
      assertEquals(1, outOfbandFenceLocalBrokerInvocations.get(key).get.size)
      assertEquals(outOfbandFenceLocalBrokerInvocations.get(key).get.head, msg1)
    })
  }

  @Test
  def testBadMetadataOffset(): Unit = {
    val listener = start(new BrokerMetadataListener(mock(classOf[KafkaConfig]), new MockTime(),
      twoProcessorsCountingInvocations(processorKeys)))
    assertEquals(expectedInitialMetadataOffset, listener.currentMetadataOffset())

    val badMetadataOffset = listener.currentMetadataOffset() - 1 // too low
    listener.put(MetadataLogEvent(List(mock(classOf[ApiMessage])), badMetadataOffset))

    val expectedNumEventsProcessedPerKey = 1 // 0 successful messages + 1 startup
    TestUtils.waitUntilTrue(() => numEventsProcessed == processorKeys.size * expectedNumEventsProcessedPerKey,
      s"Failed to process expected event before timing out; processed events = $numEventsProcessed", 5000)

    // error count metric should increase by 1 since it is a badd metadata offset and therefore no processors see it
    // we have to wait for it to occur since it is the last event and the wait above only waits for the startup events
    TestUtils.waitUntilTrue(() => errorCountGauge().value() == 1,
      s"Failed to see error count gauge increase when there was an error; error count = ${errorCountGauge().value()}", 5000)

    // offset should be unchanged
    assertEquals(expectedInitialMetadataOffset, listener.currentMetadataOffset())

    // record should not be processed
    processorKeys.foreach(key => {
      assertEquals(0, metadataLogEventInvocations.get(key).get.size)
      assertEquals(0, outOfBandRegisterLocalBrokerInvocations.get(key).get.size)
      assertEquals(0, outOfbandFenceLocalBrokerInvocations.get(key).get.size)
    })
  }

  @Test
  def testMetricsCleanedOnClose(): Unit = {
    val listener = start(new BrokerMetadataListener(mock(classOf[KafkaConfig]), new MockTime(),
      twoProcessorsCountingInvocations(processorKeys)))
    assertTrue(allRegisteredMetricNames.nonEmpty)

    listener.close()
    assertTrue(allRegisteredMetricNames.isEmpty)
  }

  @Test
  def testOutOfBandEventIsProcessed(): Unit = {
    val processedEvents = mutable.Buffer.empty[List[ApiMessage]]
    val outOfBandRegisterLocalBrokerEventIndicator = List(mock(classOf[ApiMessage]), mock(classOf[ApiMessage]))
    val outOfBandFenceLocalBrokerEventIndicator = List(mock(classOf[ApiMessage]), mock(classOf[ApiMessage]), mock(classOf[ApiMessage]))
    val countDownLatch = new CountDownLatch(1)

    val brokerMetadataProcessor = new BrokerMetadataProcessor {
      override def processStartup(): Unit = {}
      override def process(metadataLogEvent: MetadataLogEvent): Unit = {
        processedEvents += metadataLogEvent.apiMessages
        countDownLatch.await()
      }

      override def process(outOfBandRegisterLocalBrokerEvent: OutOfBandRegisterLocalBrokerEvent): Unit = {
        processedEvents += outOfBandRegisterLocalBrokerEventIndicator
      }

      override def process(outOfBandFenceLocalBrokerEvent: OutOfBandFenceLocalBrokerEvent): Unit = {
        processedEvents += outOfBandFenceLocalBrokerEventIndicator
      }
    }

    val listener = start(new BrokerMetadataListener(mock(classOf[KafkaConfig]), new MockTime(),
      List(brokerMetadataProcessor)))

    val apiMessagesEvent1 = List(mock(classOf[ApiMessage]))
    val apiMessagesEvent2 = List(mock(classOf[ApiMessage]))
    // add the out-of-band messages after the batches
    listener.put(MetadataLogEvent(apiMessagesEvent1, 1))
    listener.put(MetadataLogEvent(apiMessagesEvent2, 2))
    listener.put(OutOfBandRegisterLocalBrokerEvent(1))
    listener.put(OutOfBandFenceLocalBrokerEvent(1))
    countDownLatch.countDown()
    TestUtils.waitUntilTrue(() => processedEvents.size == 4,
      s"Failed to process expected event before timing out; processed events = ${processedEvents.size}", 5000)
    // make sure out-of-band messages processed before the second batch
    assertEquals(apiMessagesEvent1, processedEvents(0))
    assertEquals(outOfBandRegisterLocalBrokerEventIndicator, processedEvents(1))
    assertEquals(outOfBandFenceLocalBrokerEventIndicator, processedEvents(2))
    assertEquals(apiMessagesEvent2, processedEvents(3))
  }

  @Test
  def testEventQueueTime(): Unit = {
    val time = new MockTime()
    val latch = new CountDownLatch(1)
    val processedEvents = new AtomicInteger()

    val brokerMetadataProcessor = new BrokerMetadataProcessor {
      override def processStartup(): Unit = {}
      override def process(metadataLogEvent: MetadataLogEvent): Unit = {
        latch.await()
        time.sleep(500)
        processedEvents.incrementAndGet()
      }
      def process(outOfBandFenceLocalBrokerEvent: kafka.server.metadata.OutOfBandFenceLocalBrokerEvent): Unit = {}
      def process(outOfBandRegisterLocalBrokerEvent: kafka.server.metadata.OutOfBandRegisterLocalBrokerEvent): Unit = {}
    }

    // The metric should not already exist
    assertTrue(KafkaYammerMetrics.defaultRegistry.allMetrics.asScala.filter { case (k, _) =>
      k.getMBeanName == eventQueueTimeMsMetricMBeanName
    }.values.isEmpty)

    val listener = start(new BrokerMetadataListener(mock(classOf[KafkaConfig]), time,
      List(brokerMetadataProcessor)))

    val apiMessagesEvent = List(mock(classOf[ApiMessage]))
    listener.put(MetadataLogEvent(apiMessagesEvent, 1))
    listener.put(MetadataLogEvent(apiMessagesEvent, 2))
    latch.countDown()

    TestUtils.waitUntilTrue(() => processedEvents.get() == 2,
      s"Timed out waiting for processing of all events; processed events = ${processedEvents.get()}", 5000)

    val histogram = queueTimeHistogram()
    assertEquals(2, histogram.count)
    assertEquals(0, histogram.min, 0.01)
    assertEquals(500, histogram.max, 0.01)
  }

  @Test
  def testEventQueueTimeResetOnTimeout(): Unit = {
    val time = new MockTime()
    val processedEvents = new AtomicInteger()

    val brokerMetadataProcessor = new BrokerMetadataProcessor {
      override def processStartup(): Unit = {}
      override def process(metadataLogEvent: MetadataLogEvent): Unit = {
        processedEvents.incrementAndGet()
      }
      def process(outOfBandFenceLocalBrokerEvent: kafka.server.metadata.OutOfBandFenceLocalBrokerEvent): Unit = {}
      def process(outOfBandRegisterLocalBrokerEvent: kafka.server.metadata.OutOfBandRegisterLocalBrokerEvent): Unit = {}
    }

    // set a short histogram timeout of 1 ms
    val listener = start(new BrokerMetadataListener(mock(classOf[KafkaConfig]), time,
      List(brokerMetadataProcessor), 1))

    val apiMessagesEvent = List(mock(classOf[ApiMessage]))
    listener.put(MetadataLogEvent(apiMessagesEvent, 1))
    listener.put(MetadataLogEvent(apiMessagesEvent, 2))

    TestUtils.waitUntilTrue(() => processedEvents.get() == 2,
      s"Timed out waiting for processing of all events; processed eents = ${processedEvents.get()}", 5000)

    // we know from previous tests that the histogram count at this point will be 2

    // wait for the timeout/reset
    val histogram = queueTimeHistogram()
    TestUtils.waitUntilTrue(() => histogram.count == 0,
      s"Timed out on resetting $expectedEventQueueTimeMsMetricName Histogram; count=${histogram.count}", 5000)
    assertEquals(0, histogram.min, 0.1)
    assertEquals(0, histogram.max, 0.1)
  }

  def start(brokerMetadataListener: BrokerMetadataListener): BrokerMetadataListener = {
    this.brokerMetadataListener = Some(brokerMetadataListener)
    this.brokerMetadataListener.get.start()
    brokerMetadataListener
  }

  def twoProcessorsCountingInvocations(processorKeys: List[String]): List[BrokerMetadataProcessor] = {
    processorKeys.map(key => new BrokerMetadataProcessor {
      override def process(metadataLogEvent: MetadataLogEvent): Unit = {
        metadataLogEventInvocations.get(key).get += metadataLogEvent
        numEventsProcessed += 1
      }

      override def process(outOfBandRegisterLocalBrokerEvent: OutOfBandRegisterLocalBrokerEvent): Unit = {
        outOfBandRegisterLocalBrokerInvocations.get(key).get += outOfBandRegisterLocalBrokerEvent
        numEventsProcessed += 1
      }

      override def process(outOfBandFenceLocalBrokerEvent: OutOfBandFenceLocalBrokerEvent): Unit = {
        outOfbandFenceLocalBrokerInvocations.get(key).get += outOfBandFenceLocalBrokerEvent
        numEventsProcessed += 1
      }

      override def processStartup(): Unit = {
        numStartupEventsProcessed(key) = numStartupEventsProcessed.get(key).get + 1
        numEventsProcessed += 1
      }
    })
  }
}
