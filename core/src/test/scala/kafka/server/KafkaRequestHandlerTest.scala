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

import com.yammer.metrics.core.Meter
import kafka.network.RequestChannel
import org.apache.kafka.common.memory.MemoryPool
import org.apache.kafka.common.network.{ClientInformation, ListenerName}
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.{RequestContext, RequestHeader}
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.apache.kafka.common.utils.{BufferSupplier, MockTime, Time}
import org.apache.kafka.network.metrics.RequestChannelMetrics
import org.apache.kafka.server.common.RequestLocal
import org.apache.kafka.server.log.remote.storage.RemoteStorageMetrics
import org.apache.kafka.server.metrics.KafkaYammerMetrics
import org.apache.kafka.storage.log.metrics.{BrokerTopicMetrics, BrokerTopicStats}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, times, verify, when}

import java.net.InetAddress
import java.nio.ByteBuffer
import java.util.Collections
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicInteger
import java.util.stream.Collectors

class KafkaRequestHandlerTest {
  val brokerTopicStats = new BrokerTopicStats(true)
  val topic = "topic"
  val topic2 = "topic2"
  val brokerTopicMetrics: BrokerTopicMetrics = brokerTopicStats.topicStats(topic)
  val allTopicMetrics: BrokerTopicMetrics = brokerTopicStats.allTopicsStats

  @Test
  def testCallbackTiming(): Unit = {
    val time = new MockTime()
    val startTime = time.nanoseconds()
    val metrics = new RequestChannelMetrics(Collections.emptySet[ApiKeys])
    val requestChannel = new RequestChannel(10, "", time, metrics)
    val apiHandler = mock(classOf[ApiRequestHandler])
    try {
      val handler = new KafkaRequestHandler(0, 0, mock(classOf[Meter]), new AtomicInteger(1), requestChannel, apiHandler, time)

      val request = makeRequest(time, metrics)
      requestChannel.sendRequest(request)

      when(apiHandler.handle(ArgumentMatchers.eq(request), any())).thenAnswer { _ =>
        time.sleep(2)
        // Prepare the callback.
        val callback = KafkaRequestHandler.wrapAsyncCallback(
          (_: RequestLocal, ms: Int) => {
            time.sleep(ms)
            handler.stop()
          },
          RequestLocal.noCaching)
        // Execute the callback asynchronously.
        CompletableFuture.runAsync(() => callback(1))
        request.apiLocalCompleteTimeNanos = time.nanoseconds
      }

      handler.run()

      assertEquals(startTime, request.requestDequeueTimeNanos)
      assertEquals(startTime + 2000000, request.apiLocalCompleteTimeNanos)
      assertEquals(Some(startTime + 2000000), request.callbackRequestDequeueTimeNanos)
      assertEquals(Some(startTime + 3000000), request.callbackRequestCompleteTimeNanos)
    } finally {
      metrics.close()
    }
  }

  @Test
  def testCallbackTryCompleteActions(): Unit = {
    val time = new MockTime()
    val metrics = mock(classOf[RequestChannelMetrics])
    val apiHandler = mock(classOf[ApiRequestHandler])
    val requestChannel = new RequestChannel(10, "", time, metrics)
    val handler = new KafkaRequestHandler(0, 0, mock(classOf[Meter]), new AtomicInteger(1), requestChannel, apiHandler, time)

    var handledCount = 0
    var tryCompleteActionCount = 0

    val request = makeRequest(time, metrics)
    requestChannel.sendRequest(request)

    when(apiHandler.handle(ArgumentMatchers.eq(request), any())).thenAnswer { _ =>
      handledCount = handledCount + 1
      // Prepare the callback.
      val callback = KafkaRequestHandler.wrapAsyncCallback(
        (_: RequestLocal, _: Int) => {
          handler.stop()
        },
        RequestLocal.noCaching)
      // Execute the callback asynchronously.
      CompletableFuture.runAsync(() => callback(1))
    }

    when(apiHandler.tryCompleteActions()).thenAnswer { _ =>
      tryCompleteActionCount = tryCompleteActionCount + 1
    }

    handler.run()

    assertEquals(1, handledCount)
    assertEquals(1, tryCompleteActionCount)
  }

  @Test
  def testHandlingCallbackOnNewThread(): Unit = {
    val time = new MockTime()
    val metrics = mock(classOf[RequestChannelMetrics])
    val apiHandler = mock(classOf[ApiRequestHandler])
    val requestChannel = new RequestChannel(10, "", time, metrics)
    val handler = new KafkaRequestHandler(0, 0, mock(classOf[Meter]), new AtomicInteger(1), requestChannel, apiHandler, time)

    val originalRequestLocal = mock(classOf[RequestLocal])

    var handledCount = 0

    val request = makeRequest(time, metrics)
    requestChannel.sendRequest(request)

    when(apiHandler.handle(ArgumentMatchers.eq(request), any())).thenAnswer { _ =>
      // Prepare the callback.
      val callback = KafkaRequestHandler.wrapAsyncCallback(
        (reqLocal: RequestLocal, _: Int) => {
          reqLocal.bufferSupplier.close()
          handledCount = handledCount + 1
          handler.stop()
        },
        originalRequestLocal)
      // Execute the callback asynchronously.
      CompletableFuture.runAsync(() => callback(1))
    }

    handler.run()
    // Verify that we don't use the request local that we passed in.
    verify(originalRequestLocal, times(0)).bufferSupplier
    assertEquals(1, handledCount)
  }

  @Test
  def testCallbackOnSameThread(): Unit = {
    val time = new MockTime()
    val metrics = mock(classOf[RequestChannelMetrics])
    val apiHandler = mock(classOf[ApiRequestHandler])
    val requestChannel = new RequestChannel(10, "", time, metrics)
    val handler = new KafkaRequestHandler(0, 0, mock(classOf[Meter]), new AtomicInteger(1), requestChannel, apiHandler, time)

    val originalRequestLocal = mock(classOf[RequestLocal])
    when(originalRequestLocal.bufferSupplier).thenReturn(BufferSupplier.create())

    var handledCount = 0

    val request = makeRequest(time, metrics)
    requestChannel.sendRequest(request)

    when(apiHandler.handle(ArgumentMatchers.eq(request), any())).thenAnswer { _ =>
      // Prepare the callback.
      val callback = KafkaRequestHandler.wrapAsyncCallback(
        (reqLocal: RequestLocal, _: Int) => {
          reqLocal.bufferSupplier.close()
          handledCount = handledCount + 1
          handler.stop()
        },
        originalRequestLocal)
      // Execute the callback before the request returns.
      callback(1)
    }

    handler.run()
    // Verify that we do use the request local that we passed in.
    verify(originalRequestLocal, times(1)).bufferSupplier
    assertEquals(1, handledCount)
  }


  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testTopicStats(systemRemoteStorageEnabled: Boolean): Unit = {
    val brokerTopicStats = new BrokerTopicStats(systemRemoteStorageEnabled)
    val brokerTopicMetrics = brokerTopicStats.topicStats(topic)
    val gaugeMetrics = Set(
      RemoteStorageMetrics.REMOTE_LOG_SIZE_COMPUTATION_TIME_METRIC.getName,
      RemoteStorageMetrics.REMOTE_COPY_LAG_BYTES_METRIC.getName,
      RemoteStorageMetrics.REMOTE_COPY_LAG_SEGMENTS_METRIC.getName,
      RemoteStorageMetrics.REMOTE_DELETE_LAG_BYTES_METRIC.getName,
      RemoteStorageMetrics.REMOTE_DELETE_LAG_SEGMENTS_METRIC.getName,
      RemoteStorageMetrics.REMOTE_LOG_SIZE_COMPUTATION_TIME_METRIC.getName,
      RemoteStorageMetrics.REMOTE_LOG_METADATA_COUNT_METRIC.getName,
      RemoteStorageMetrics.REMOTE_LOG_SIZE_BYTES_METRIC.getName)

    RemoteStorageMetrics.brokerTopicStatsMetrics.forEach(metric => {
      if (systemRemoteStorageEnabled) {
        if (!gaugeMetrics.contains(metric.getName)) {
          assertTrue(brokerTopicMetrics.metricMapKeySet().contains(metric.getName), "the metric is missing: " + metric.getName)
        } else {
          assertFalse(brokerTopicMetrics.metricMapKeySet().contains(metric.getName), "the metric should not appear: " + metric.getName)
        }
      } else {
        assertFalse(brokerTopicMetrics.metricMapKeySet().contains(metric.getName))
      }
    })
    gaugeMetrics.foreach(metricName => {
      if (systemRemoteStorageEnabled) {
        assertTrue(brokerTopicMetrics.metricGaugeMap.containsKey(metricName), "The metric is missing:" + metricName)
      } else {
        assertFalse(brokerTopicMetrics.metricGaugeMap.containsKey(metricName), "The metric should appear:" + metricName)
      }
    })
  }

  def makeRequest(time: Time, metrics: RequestChannelMetrics): RequestChannel.Request = {
    // Make unsupported API versions request to avoid having to parse a real request
    val requestHeader = mock(classOf[RequestHeader])
    when(requestHeader.apiKey()).thenReturn(ApiKeys.API_VERSIONS)
    when(requestHeader.apiVersion()).thenReturn(0.toShort)

    val context = new RequestContext(requestHeader, "0", mock(classOf[InetAddress]), new KafkaPrincipal("", ""),
      new ListenerName(""), SecurityProtocol.PLAINTEXT, mock(classOf[ClientInformation]), false)
    new RequestChannel.Request(0, context, time.nanoseconds(),
      mock(classOf[MemoryPool]), ByteBuffer.allocate(0), metrics)
  }

  def setupBrokerTopicMetrics(systemRemoteStorageEnabled: Boolean = true): BrokerTopicMetrics = {
    val topic = "topic"
    new BrokerTopicMetrics(topic, systemRemoteStorageEnabled)
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testSingularCopyLagBytesMetric(systemRemoteStorageEnabled: Boolean): Unit = {
    val brokerTopicStats = new BrokerTopicStats(systemRemoteStorageEnabled)
    val brokerTopicMetrics = brokerTopicStats.topicStats(topic)

    if (systemRemoteStorageEnabled) {
      brokerTopicStats.recordRemoteCopyLagBytes(topic, 0, 100)
      brokerTopicStats.recordRemoteCopyLagBytes(topic, 1, 150)
      brokerTopicStats.recordRemoteCopyLagBytes(topic, 2, 250)
      assertEquals(500, brokerTopicMetrics.remoteCopyLagBytes)
      assertEquals(500, brokerTopicStats.allTopicsStats.remoteCopyLagBytes)
      brokerTopicStats.recordRemoteCopyLagBytes(topic2, 0, 100)
      assertEquals(600, brokerTopicStats.allTopicsStats.remoteCopyLagBytes)
    } else {
      assertEquals(null, brokerTopicMetrics.metricGaugeMap.get(RemoteStorageMetrics.REMOTE_COPY_LAG_BYTES_METRIC.getName))
      assertEquals(null, brokerTopicStats.allTopicsStats.metricGaugeMap.get(RemoteStorageMetrics.REMOTE_COPY_LAG_BYTES_METRIC.getName))
    }
  }

  @Test
  def testMultipleCopyLagBytesMetrics(): Unit = {
    brokerTopicStats.recordRemoteCopyLagBytes(topic, 0, 1)
    brokerTopicStats.recordRemoteCopyLagBytes(topic, 1, 2)
    brokerTopicStats.recordRemoteCopyLagBytes(topic, 2, 3)

    brokerTopicStats.recordRemoteCopyLagBytes(topic, 0, 4)
    brokerTopicStats.recordRemoteCopyLagBytes(topic, 1, 5)
    brokerTopicStats.recordRemoteCopyLagBytes(topic, 2, 6)

    assertEquals(15, brokerTopicMetrics.remoteCopyLagBytes)
    assertEquals(15, allTopicMetrics.remoteCopyLagBytes)
    brokerTopicStats.recordRemoteCopyLagBytes(topic2, 2, 5)
    assertEquals(20, allTopicMetrics.remoteCopyLagBytes)
  }

  @Test
  def testCopyLagBytesMetricWithPartitionExpansion(): Unit = {
    brokerTopicStats.recordRemoteCopyLagBytes(topic, 0, 1)
    brokerTopicStats.recordRemoteCopyLagBytes(topic, 1, 2)

    assertEquals(3, brokerTopicMetrics.remoteCopyLagBytes)
    assertEquals(3, allTopicMetrics.remoteCopyLagBytes)

    brokerTopicStats.recordRemoteCopyLagBytes(topic, 2, 3)

    assertEquals(6, brokerTopicMetrics.remoteCopyLagBytes)
    assertEquals(6, allTopicMetrics.remoteCopyLagBytes)
    brokerTopicStats.recordRemoteCopyLagBytes(topic2, 0, 1)
    assertEquals(7, allTopicMetrics.remoteCopyLagBytes)
  }

  @Test
  def testCopyLagBytesMetricWithPartitionShrinking(): Unit = {
    brokerTopicStats.recordRemoteCopyLagBytes(topic, 0, 1)
    brokerTopicStats.recordRemoteCopyLagBytes(topic, 1, 2)

    assertEquals(3, brokerTopicMetrics.remoteCopyLagBytes)
    assertEquals(3, allTopicMetrics.remoteCopyLagBytes)

    brokerTopicStats.removeRemoteCopyLagBytes(topic, 1)

    assertEquals(1, brokerTopicMetrics.remoteCopyLagBytes)
    assertEquals(1, allTopicMetrics.remoteCopyLagBytes)

    brokerTopicStats.recordRemoteCopyLagBytes(topic2, 0, 1)
    assertEquals(2, allTopicMetrics.remoteCopyLagBytes)
  }

  @Test
  def testCopyLagBytesMetricWithRemovingNonexistentPartitions(): Unit = {
    brokerTopicStats.recordRemoteCopyLagBytes(topic, 0, 1)
    brokerTopicStats.recordRemoteCopyLagBytes(topic, 1, 2)

    assertEquals(3, brokerTopicMetrics.remoteCopyLagBytes)
    assertEquals(3, allTopicMetrics.remoteCopyLagBytes)


    brokerTopicStats.removeRemoteCopyLagBytes(topic, 3)

    assertEquals(3, brokerTopicMetrics.remoteCopyLagBytes)
    assertEquals(3, allTopicMetrics.remoteCopyLagBytes)
  }

  @Test
  def testCopyLagBytesMetricClear(): Unit = {
    brokerTopicStats.recordRemoteCopyLagBytes(topic, 0, 1)
    brokerTopicStats.recordRemoteCopyLagBytes(topic, 1, 2)

    assertEquals(3, brokerTopicMetrics.remoteCopyLagBytes)
    assertEquals(3, allTopicMetrics.remoteCopyLagBytes)

    brokerTopicStats.close()

    assertEquals(0, brokerTopicMetrics.remoteCopyLagBytes)
    assertEquals(0, allTopicMetrics.remoteCopyLagBytes)

    brokerTopicStats.recordRemoteCopyLagBytes(topic2, 0, 1)
    assertEquals(1, allTopicMetrics.remoteCopyLagBytes)
  }

  @Test
  def testMultipleCopyLagSegmentsMetrics(): Unit = {
    brokerTopicStats.recordRemoteCopyLagSegments(topic, 0, 1)
    brokerTopicStats.recordRemoteCopyLagSegments(topic, 1, 2)
    brokerTopicStats.recordRemoteCopyLagSegments(topic, 2, 3)

    brokerTopicStats.recordRemoteCopyLagSegments(topic, 0, 4)
    brokerTopicStats.recordRemoteCopyLagSegments(topic, 1, 5)
    brokerTopicStats.recordRemoteCopyLagSegments(topic, 2, 6)

    assertEquals(15, brokerTopicMetrics.remoteCopyLagSegments)
    assertEquals(15, allTopicMetrics.remoteCopyLagSegments)

    brokerTopicStats.recordRemoteCopyLagSegments(topic2, 0, 1)
    assertEquals(16, allTopicMetrics.remoteCopyLagSegments)
  }

  @Test
  def testCopyLagSegmentsMetricWithPartitionExpansion(): Unit = {
    brokerTopicStats.recordRemoteCopyLagSegments(topic, 0, 1)
    brokerTopicStats.recordRemoteCopyLagSegments(topic, 1, 2)

    assertEquals(3, brokerTopicMetrics.remoteCopyLagSegments)
    assertEquals(3, allTopicMetrics.remoteCopyLagSegments)

    brokerTopicStats.recordRemoteCopyLagSegments(topic, 2, 3)

    assertEquals(6, brokerTopicMetrics.remoteCopyLagSegments)
    assertEquals(6, allTopicMetrics.remoteCopyLagSegments)
  }

  @Test
  def testCopyLagSegmentsMetricWithPartitionShrinking(): Unit = {
    brokerTopicStats.recordRemoteCopyLagSegments(topic, 0, 1)
    brokerTopicStats.recordRemoteCopyLagSegments(topic, 1, 2)

    assertEquals(3, brokerTopicMetrics.remoteCopyLagSegments)
    assertEquals(3, allTopicMetrics.remoteCopyLagSegments)

    brokerTopicStats.removeRemoteCopyLagSegments(topic, 1)

    assertEquals(1, brokerTopicMetrics.remoteCopyLagSegments)
    assertEquals(1, allTopicMetrics.remoteCopyLagSegments)
  }

  @Test
  def testCopyLagSegmentsMetricWithRemovingNonexistentPartitions(): Unit = {
    brokerTopicStats.recordRemoteCopyLagSegments(topic, 0, 1)
    brokerTopicStats.recordRemoteCopyLagSegments(topic, 1, 2)

    assertEquals(3, brokerTopicMetrics.remoteCopyLagSegments)
    assertEquals(3, allTopicMetrics.remoteCopyLagSegments)

    brokerTopicStats.removeRemoteCopyLagSegments(topic, 3)

    assertEquals(3, brokerTopicMetrics.remoteCopyLagSegments)
    assertEquals(3, allTopicMetrics.remoteCopyLagSegments)
  }

  @Test
  def testCopyLagSegmentsMetricClear(): Unit = {
    brokerTopicStats.recordRemoteCopyLagSegments(topic, 0, 1)
    brokerTopicStats.recordRemoteCopyLagSegments(topic, 1, 2)

    assertEquals(3, brokerTopicMetrics.remoteCopyLagSegments)
    assertEquals(3, allTopicMetrics.remoteCopyLagSegments)

    brokerTopicStats.close()

    assertEquals(0, brokerTopicMetrics.remoteCopyLagSegments)
    assertEquals(0, allTopicMetrics.remoteCopyLagSegments)

  }

  @Test
  def testMultipleDeleteLagBytesMetrics(): Unit = {
    brokerTopicStats.recordRemoteDeleteLagBytes(topic, 0, 1)
    brokerTopicStats.recordRemoteDeleteLagBytes(topic, 1, 2)
    brokerTopicStats.recordRemoteDeleteLagBytes(topic, 2, 3)

    brokerTopicStats.recordRemoteDeleteLagBytes(topic, 0, 4)
    brokerTopicStats.recordRemoteDeleteLagBytes(topic, 1, 5)
    brokerTopicStats.recordRemoteDeleteLagBytes(topic, 2, 6)

    assertEquals(15, brokerTopicMetrics.remoteDeleteLagBytes)
    assertEquals(15, allTopicMetrics.remoteDeleteLagBytes)

    brokerTopicStats.recordRemoteDeleteLagBytes(topic2, 0, 1)
    assertEquals(16, allTopicMetrics.remoteDeleteLagBytes)
  }

  @Test
  def testDeleteLagBytesMetricWithPartitionExpansion(): Unit = {
    brokerTopicStats.recordRemoteDeleteLagBytes(topic, 0, 1)
    brokerTopicStats.recordRemoteDeleteLagBytes(topic, 1, 2)

    assertEquals(3, brokerTopicMetrics.remoteDeleteLagBytes)
    assertEquals(3, allTopicMetrics.remoteDeleteLagBytes)

    brokerTopicStats.recordRemoteDeleteLagBytes(topic, 2, 3)

    assertEquals(6, brokerTopicMetrics.remoteDeleteLagBytes)
    assertEquals(6, allTopicMetrics.remoteDeleteLagBytes)
  }

  @Test
  def testDeleteLagBytesMetricWithPartitionShrinking(): Unit = {
    brokerTopicStats.recordRemoteDeleteLagBytes(topic, 0, 1)
    brokerTopicStats.recordRemoteDeleteLagBytes(topic, 1, 2)

    assertEquals(3, brokerTopicMetrics.remoteDeleteLagBytes)
    assertEquals(3, allTopicMetrics.remoteDeleteLagBytes)

    brokerTopicStats.removeRemoteDeleteLagBytes(topic, 1)

    assertEquals(1, brokerTopicMetrics.remoteDeleteLagBytes)
    assertEquals(1, allTopicMetrics.remoteDeleteLagBytes)
  }

  @Test
  def testDeleteLagBytesMetricWithRemovingNonexistentPartitions(): Unit = {
    brokerTopicStats.recordRemoteDeleteLagBytes(topic, 0, 1)
    brokerTopicStats.recordRemoteDeleteLagBytes(topic, 1, 2)

    assertEquals(3, brokerTopicMetrics.remoteDeleteLagBytes)
    assertEquals(3, allTopicMetrics.remoteDeleteLagBytes)

    brokerTopicStats.removeRemoteDeleteLagBytes(topic, 3)

    assertEquals(3, brokerTopicMetrics.remoteDeleteLagBytes)
    assertEquals(3, allTopicMetrics.remoteDeleteLagBytes)
  }

  @Test
  def testDeleteLagBytesMetricClear(): Unit = {
    brokerTopicStats.recordRemoteDeleteLagBytes(topic, 0, 1)
    brokerTopicStats.recordRemoteDeleteLagBytes(topic, 1, 2)

    assertEquals(3, brokerTopicMetrics.remoteDeleteLagBytes)
    assertEquals(3, allTopicMetrics.remoteDeleteLagBytes)

    brokerTopicStats.close()

    assertEquals(0, brokerTopicMetrics.remoteDeleteLagBytes)
    assertEquals(0, allTopicMetrics.remoteDeleteLagBytes)
  }

  @Test
  def testMultipleDeleteLagSegmentsMetrics(): Unit = {
    brokerTopicStats.recordRemoteDeleteLagSegments(topic, 0, 1)
    brokerTopicStats.recordRemoteDeleteLagSegments(topic, 1, 2)
    brokerTopicStats.recordRemoteDeleteLagSegments(topic, 2, 3)

    brokerTopicStats.recordRemoteDeleteLagSegments(topic, 0, 4)
    brokerTopicStats.recordRemoteDeleteLagSegments(topic, 1, 5)
    brokerTopicStats.recordRemoteDeleteLagSegments(topic, 2, 6)

    assertEquals(15, brokerTopicMetrics.remoteDeleteLagSegments)
    assertEquals(15, allTopicMetrics.remoteDeleteLagSegments)

    brokerTopicStats.recordRemoteDeleteLagSegments(topic2, 1, 5)
    assertEquals(20, allTopicMetrics.remoteDeleteLagSegments)
  }

  @Test
  def testDeleteLagSegmentsMetricWithPartitionExpansion(): Unit = {
    brokerTopicStats.recordRemoteDeleteLagSegments(topic, 0, 1)
    brokerTopicStats.recordRemoteDeleteLagSegments(topic, 1, 2)

    assertEquals(3, brokerTopicMetrics.remoteDeleteLagSegments)
    assertEquals(3, allTopicMetrics.remoteDeleteLagSegments)

    brokerTopicStats.recordRemoteDeleteLagSegments(topic, 2, 3)

    assertEquals(6, brokerTopicMetrics.remoteDeleteLagSegments)
    assertEquals(6, allTopicMetrics.remoteDeleteLagSegments)
  }

  @Test
  def testDeleteLagSegmentsMetricWithPartitionShrinking(): Unit = {
    brokerTopicStats.recordRemoteDeleteLagSegments(topic, 0, 1)
    brokerTopicStats.recordRemoteDeleteLagSegments(topic, 1, 2)

    assertEquals(3, brokerTopicMetrics.remoteDeleteLagSegments)
    assertEquals(3, allTopicMetrics.remoteDeleteLagSegments)

    brokerTopicStats.removeRemoteDeleteLagSegments(topic, 1)

    assertEquals(1, brokerTopicMetrics.remoteDeleteLagSegments)
    assertEquals(1, allTopicMetrics.remoteDeleteLagSegments)
  }

  @Test
  def testDeleteLagSegmentsMetricWithRemovingNonexistentPartitions(): Unit = {
    brokerTopicStats.recordRemoteDeleteLagSegments(topic, 0, 1)
    brokerTopicStats.recordRemoteDeleteLagSegments(topic, 1, 2)

    assertEquals(3, brokerTopicMetrics.remoteDeleteLagSegments)
    assertEquals(3, allTopicMetrics.remoteDeleteLagSegments)

    brokerTopicStats.removeRemoteDeleteLagSegments(topic, 3)

    assertEquals(3, brokerTopicMetrics.remoteDeleteLagSegments)
    assertEquals(3, allTopicMetrics.remoteDeleteLagSegments)
  }

  @Test
  def testDeleteLagSegmentsMetricClear(): Unit = {
    brokerTopicStats.recordRemoteDeleteLagSegments(topic, 0, 1)
    brokerTopicStats.recordRemoteDeleteLagSegments(topic, 1, 2)

    assertEquals(3, brokerTopicMetrics.remoteDeleteLagSegments)
    assertEquals(3, allTopicMetrics.remoteDeleteLagSegments)

    brokerTopicStats.close()

    assertEquals(0, brokerTopicMetrics.remoteDeleteLagSegments)
    assertEquals(0, allTopicMetrics.remoteDeleteLagSegments)
  }

  @Test
  def testRemoteLogMetadataCount(): Unit = {
    assertEquals(0, brokerTopicMetrics.remoteLogMetadataCount)
    assertEquals(0, allTopicMetrics.remoteLogMetadataCount)
    brokerTopicStats.recordRemoteLogMetadataCount(topic, 0, 1)
    assertEquals(1, brokerTopicMetrics.remoteLogMetadataCount)
    assertEquals(1, allTopicMetrics.remoteLogMetadataCount)

    brokerTopicStats.recordRemoteLogMetadataCount(topic, 1, 2)
    brokerTopicStats.recordRemoteLogMetadataCount(topic, 2, 3)
    assertEquals(6, brokerTopicMetrics.remoteLogMetadataCount)
    assertEquals(6, allTopicMetrics.remoteLogMetadataCount)

    brokerTopicStats.close()

    assertEquals(0, brokerTopicMetrics.remoteLogMetadataCount)
    assertEquals(0, allTopicMetrics.remoteLogMetadataCount)
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testSingularLogSizeBytesMetric(systemRemoteStorageEnabled: Boolean): Unit = {
    val brokerTopicStats = new BrokerTopicStats(systemRemoteStorageEnabled)
    val brokerTopicMetrics = brokerTopicStats.topicStats(topic)
    if (systemRemoteStorageEnabled) {
      brokerTopicStats.recordRemoteLogSizeBytes(topic, 0, 100)
      brokerTopicStats.recordRemoteLogSizeBytes(topic, 1, 150)
      brokerTopicStats.recordRemoteLogSizeBytes(topic, 2, 250)
      assertEquals(500, brokerTopicMetrics.remoteLogSizeBytes)
      assertEquals(500, brokerTopicStats.allTopicsStats.remoteLogSizeBytes)

      brokerTopicStats.recordRemoteLogSizeBytes(topic2, 0, 100)
      assertEquals(600, brokerTopicStats.allTopicsStats.remoteLogSizeBytes)
    } else {
      assertEquals(null, brokerTopicMetrics.metricGaugeMap.get(RemoteStorageMetrics.REMOTE_LOG_SIZE_BYTES_METRIC.getName))
    }
  }

  @Test
  def testMultipleLogSizeBytesMetrics(): Unit = {
    brokerTopicStats.recordRemoteLogSizeBytes(topic, 0, 1)
    brokerTopicStats.recordRemoteLogSizeBytes(topic, 1, 2)
    brokerTopicStats.recordRemoteLogSizeBytes(topic, 2, 3)

    brokerTopicStats.recordRemoteLogSizeBytes(topic, 0, 4)
    brokerTopicStats.recordRemoteLogSizeBytes(topic, 1, 5)
    brokerTopicStats.recordRemoteLogSizeBytes(topic, 2, 6)

    assertEquals(15, brokerTopicMetrics.remoteLogSizeBytes)
    assertEquals(15, allTopicMetrics.remoteLogSizeBytes)
  }

  @Test
  def testLogSizeBytesMetricWithPartitionExpansion(): Unit = {
    brokerTopicStats.recordRemoteLogSizeBytes(topic, 0, 1)
    brokerTopicStats.recordRemoteLogSizeBytes(topic, 1, 2)

    assertEquals(3, brokerTopicMetrics.remoteLogSizeBytes)
    assertEquals(3, allTopicMetrics.remoteLogSizeBytes)

    brokerTopicStats.recordRemoteLogSizeBytes(topic, 2, 3)

    assertEquals(6, brokerTopicMetrics.remoteLogSizeBytes)
    assertEquals(6, allTopicMetrics.remoteLogSizeBytes)
  }

  @Test
  def testLogSizeBytesMetricWithPartitionShrinking(): Unit = {
    brokerTopicStats.recordRemoteLogSizeBytes(topic, 0, 1)
    brokerTopicStats.recordRemoteLogSizeBytes(topic, 1, 2)

    assertEquals(3, brokerTopicMetrics.remoteLogSizeBytes)
    assertEquals(3, allTopicMetrics.remoteLogSizeBytes)

    brokerTopicStats.removeRemoteLogSizeBytes(topic, 1)

    assertEquals(1, brokerTopicMetrics.remoteLogSizeBytes)
    assertEquals(1, allTopicMetrics.remoteLogSizeBytes)
  }

  @Test
  def testLogSizeBytesMetricWithRemovingNonexistentPartitions(): Unit = {
    brokerTopicStats.recordRemoteLogSizeBytes(topic, 0, 1)
    brokerTopicStats.recordRemoteLogSizeBytes(topic, 1, 2)

    assertEquals(3, brokerTopicMetrics.remoteLogSizeBytes)
    assertEquals(3, allTopicMetrics.remoteLogSizeBytes)

    brokerTopicStats.removeRemoteLogSizeBytes(topic, 3)

    assertEquals(3, brokerTopicMetrics.remoteLogSizeBytes)
    assertEquals(3, allTopicMetrics.remoteLogSizeBytes)
  }

  @Test
  def testLogSizeBytesMetricClear(): Unit = {
    brokerTopicStats.recordRemoteLogSizeBytes(topic, 0, 1)
    brokerTopicStats.recordRemoteLogSizeBytes(topic, 1, 2)

    assertEquals(3, brokerTopicMetrics.remoteLogSizeBytes)
    assertEquals(3, allTopicMetrics.remoteLogSizeBytes)

    brokerTopicStats.close()

    assertEquals(0, brokerTopicMetrics.remoteLogSizeBytes)
    assertEquals(0, allTopicMetrics.remoteLogSizeBytes)
  }

  @Test
  def testGaugeClose(): Unit = {
    val brokerTopicStats = new BrokerTopicStats(true)
    val topic = "close-test-topic"
    val brokerTopicMetrics: BrokerTopicMetrics = brokerTopicStats.topicStats(topic)
    assertEquals(7, KafkaYammerMetrics.defaultRegistry.allMetrics().keySet().stream().filter(metricName => metricName.getMBeanName.contains(s"topic=$topic")).collect(Collectors.toList()).size())

    brokerTopicMetrics.close()
    assertEquals(0, KafkaYammerMetrics.defaultRegistry.allMetrics().keySet().stream().filter(metricName => metricName.getMBeanName.contains(s"topic=$topic")).collect(Collectors.toList()).size())

    brokerTopicStats.recordRemoteCopyLagBytes(topic, 0, 1)
    brokerTopicStats.recordRemoteCopyLagSegments(topic, 0, 1)
    brokerTopicStats.recordRemoteDeleteLagBytes(topic, 0, 1)
    brokerTopicStats.recordRemoteDeleteLagSegments(topic, 0, 1)
    brokerTopicStats.recordRemoteLogMetadataCount(topic, 0, 1)
    brokerTopicStats.recordRemoteLogSizeComputationTime(topic, 0, 1)
    brokerTopicStats.recordRemoteLogSizeBytes(topic, 0, 1)
    assertEquals(7, KafkaYammerMetrics.defaultRegistry.allMetrics().keySet().stream().filter(metricName => metricName.getMBeanName.contains(s"topic=$topic")).collect(Collectors.toList()).size())

    // cleanup
    brokerTopicStats.close()
  }
}
