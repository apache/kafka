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

import com.yammer.metrics.core.{Gauge, Histogram, MetricName}
import kafka.metrics.KafkaYammerMetrics
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.apache.kafka.common.metadata.BrokerRecord
import org.apache.kafka.common.protocol.ApiMessage
import org.apache.kafka.common.utils.MockTime
import org.junit.Assert.{assertEquals, assertTrue, fail}
import org.junit.{After, Test}
import org.mockito.Mockito.mock
import org.scalatest.Assertions.intercept

import scala.collection.mutable
import scala.jdk.CollectionConverters._

class BrokerMetadataListenerTest {
  val expectedMetricMBeanPrefix = "kafka.server.metadata:type=BrokerMetadataListener"
  val expectedEventQueueTimeMsMetricName = "EventQueueTimeMs"
  val expectedEventQueueSizeMetricName = "EventQueueSize"
  val eventQueueSizeMetricMBeanName = s"$expectedMetricMBeanPrefix,name=$expectedEventQueueSizeMetricName"
  val eventQueueTimeMsMetricMBeanName = s"$expectedMetricMBeanPrefix,name=$expectedEventQueueTimeMsMetricName"

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

  val expectedInitialMetadataOffset = -1

  @After
  def clearMetrics(): Unit = {
    TestUtils.clearYammerMetrics()
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testEmptyBrokerMetadataProcessors(): Unit = {
    new BrokerMetadataListener(mock(classOf[KafkaConfig]), new MockTime(), List.empty)
  }

  @Test
  def testEventIsProcessedAfterStartup(): Unit = {
    val processor = new MockMetadataProcessor
    val listener = new BrokerMetadataListener(mock(classOf[KafkaConfig]), new MockTime(), List(processor))

    val metadataLogEvent = MetadataLogEvent(List[ApiMessage](new BrokerRecord()).asJava, 1)
    listener.put(metadataLogEvent)
    listener.drain()
    assertEquals(List(metadataLogEvent), processor.processed.toList)
  }

  @Test
  def testInitialAndSubsequentMetadataOffsets(): Unit = {
    val processor = new MockMetadataProcessor
    val listener = new BrokerMetadataListener(mock(classOf[KafkaConfig]), new MockTime(), List(processor))
    assertEquals(expectedInitialMetadataOffset, listener.currentMetadataOffset())

    val nextMetadataOffset = expectedInitialMetadataOffset + 2
    val msg0 = mock(classOf[ApiMessage])
    val msg1 = mock(classOf[ApiMessage])
    val apiMessages = List(msg0, msg1)
    val event = MetadataLogEvent(apiMessages.asJava, nextMetadataOffset)
    listener.put(event)
    listener.drain()
    assertEquals(List(event), processor.processed.toList)
    assertEquals(nextMetadataOffset, listener.currentMetadataOffset())
  }

  @Test
  def testOutOfBandHeartbeatMessages(): Unit = {
    val processor = new MockMetadataProcessor
    val listener = new BrokerMetadataListener(mock(classOf[KafkaConfig]), new MockTime(), List(processor))
    assertEquals(expectedInitialMetadataOffset, listener.currentMetadataOffset())

    val msg0 = RegisterBrokerEvent(1)
    val msg1 = FenceBrokerEvent(1)
    listener.put(msg0)
    listener.put(msg1)
    listener.drain()
    assertEquals(List(msg0, msg1), processor.processed.toList)

    // offset should not be updated
    assertEquals(expectedInitialMetadataOffset, listener.currentMetadataOffset())
  }

  @Test
  def testBadMetadataOffset(): Unit = {
    val processor = new MockMetadataProcessor
    val listener = new BrokerMetadataListener(mock(classOf[KafkaConfig]), new MockTime(), List(processor))
    assertEquals(expectedInitialMetadataOffset, listener.currentMetadataOffset())

    val metadataLogEvent = MetadataLogEvent(List[ApiMessage](new BrokerRecord()).asJava, -1)
    listener.put(metadataLogEvent)

    intercept[IllegalStateException] {
      listener.drain()
    }

    // offset should be unchanged
    assertEquals(expectedInitialMetadataOffset, listener.currentMetadataOffset())

    // record should not be processed
    assertEquals(List.empty, processor.processed.toList)
  }

  @Test
  def testMetricsCleanedOnClose(): Unit = {
    val listener = new BrokerMetadataListener(mock(classOf[KafkaConfig]), new MockTime(),
      List(new MockMetadataProcessor))
    listener.start()
    assertTrue(allRegisteredMetricNames.nonEmpty)

    listener.close()
    assertTrue(allRegisteredMetricNames.isEmpty)
  }

  @Test
  def testOutOfBandEventIsProcessed(): Unit = {
    val processor = new MockMetadataProcessor
    val listener = new BrokerMetadataListener(mock(classOf[KafkaConfig]), new MockTime(), List(processor))
    val logEvent1 = MetadataLogEvent(List(mock(classOf[ApiMessage])).asJava, 1)
    val logEvent2 = MetadataLogEvent(List(mock(classOf[ApiMessage])).asJava, 2)
    val registerEvent = RegisterBrokerEvent(1)
    val fenceEvent = FenceBrokerEvent(1)

    // add the out-of-band messages after the batches
    listener.put(logEvent1)
    listener.put(logEvent2)
    listener.put(registerEvent)
    listener.put(fenceEvent)
    listener.drain()

    // make sure events are handled in order
    assertEquals(List(logEvent1, logEvent2, registerEvent, fenceEvent), processor.processed.toList)
  }

  @Test
  def testEventQueueTime(): Unit = {
    val time = new MockTime()
    val brokerMetadataProcessor = new MockMetadataProcessor

    // The metric should not already exist
    assertTrue(KafkaYammerMetrics.defaultRegistry.allMetrics.asScala.filter { case (k, _) =>
      k.getMBeanName == eventQueueTimeMsMetricMBeanName
    }.values.isEmpty)

    val listener = new BrokerMetadataListener(mock(classOf[KafkaConfig]), time, List(brokerMetadataProcessor))
    val apiMessagesEvent = List(mock(classOf[ApiMessage]))
    listener.put(MetadataLogEvent(apiMessagesEvent.asJava, 1))
    listener.drain()

    listener.put(MetadataLogEvent(apiMessagesEvent.asJava, 2))
    time.sleep(500)
    listener.drain()

    val histogram = queueTimeHistogram()
    assertEquals(2, histogram.count)
    assertEquals(0, histogram.min, 0.01)
    assertEquals(500, histogram.max, 0.01)
  }

  @Test
  def testEventQueueHistogramResetAfterTimeout(): Unit = {
    val time = new MockTime()
    val brokerMetadataProcessor = new MockMetadataProcessor

    val listener = new BrokerMetadataListener(mock(classOf[KafkaConfig]), time, List(brokerMetadataProcessor),
      eventQueueTimeoutMs = 50)
    val histogram = queueTimeHistogram()

    val metadataLogEvent = MetadataLogEvent(List[ApiMessage](new BrokerRecord()).asJava, 1)
    listener.put(metadataLogEvent)
    listener.drain()
    assertEquals(1, histogram.count())

    listener.poll()
    assertEquals(0, histogram.count())
  }

  private class MockMetadataProcessor extends BrokerMetadataProcessor {
    val processed: mutable.Buffer[BrokerMetadataEvent] = mutable.Buffer.empty

    override def process(event: BrokerMetadataEvent): Unit = {
      processed += event
    }
  }

}
