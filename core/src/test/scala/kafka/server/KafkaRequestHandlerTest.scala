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
import org.apache.kafka.common.utils.internals.BufferSupplier
import org.apache.kafka.common.utils.{MockTime, Time}
import org.apache.kafka.network.Request
import org.apache.kafka.network.metrics.RequestChannelMetrics
import org.apache.kafka.server.common.RequestLocal
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, times, verify, when}

import java.net.InetAddress
import java.nio.ByteBuffer
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicInteger

class KafkaRequestHandlerTest {

  @Test
  def testCallbackTiming(): Unit = {
    val time = new MockTime()
    val startTime = time.nanoseconds()
    val metrics = new RequestChannelMetrics(java.util.Set.of[ApiKeys])
    val requestChannel = new RequestChannel(10, time, metrics)
    val apiHandler = mock(classOf[ApiRequestHandler])
    try {
      val handler = new KafkaRequestHandler(0, 0, mock(classOf[Meter]), new AtomicInteger(1), mock(classOf[Meter]), new AtomicInteger(1), requestChannel, apiHandler, time, "broker")

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
        request.apiLocalCompleteTimeNanos(time.nanoseconds)
      }

      handler.run()

      assertEquals(startTime, request.requestDequeueTimeNanos)
      assertEquals(startTime + 2000000, request.apiLocalCompleteTimeNanos)
      assertEquals(startTime + 2000000, request.callbackRequestDequeueTimeNanos.getAsLong)
      assertEquals(startTime + 3000000, request.callbackRequestCompleteTimeNanos.getAsLong)
    } finally {
      metrics.close()
    }
  }

  @Test
  def testCallbackTryCompleteActions(): Unit = {
    val time = new MockTime()
    val metrics = mock(classOf[RequestChannelMetrics])
    val apiHandler = mock(classOf[ApiRequestHandler])
    val requestChannel = new RequestChannel(10, time, metrics)
    val handler = new KafkaRequestHandler(0, 0, mock(classOf[Meter]), new AtomicInteger(1), mock(classOf[Meter]), new AtomicInteger(1), requestChannel, apiHandler, time, "broker")

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
    val requestChannel = new RequestChannel(10, time, metrics)
    val handler = new KafkaRequestHandler(0, 0, mock(classOf[Meter]), new AtomicInteger(1), mock(classOf[Meter]), new AtomicInteger(1), requestChannel, apiHandler, time, "broker")

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
    val requestChannel = new RequestChannel(10, time, metrics)
    val handler = new KafkaRequestHandler(0, 0, mock(classOf[Meter]), new AtomicInteger(1), mock(classOf[Meter]), new AtomicInteger(1), requestChannel, apiHandler, time, "broker")

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

  def makeRequest(time: Time, metrics: RequestChannelMetrics): Request = {
    // Make unsupported API versions request to avoid having to parse a real request
    val requestHeader = mock(classOf[RequestHeader])
    when(requestHeader.apiKey()).thenReturn(ApiKeys.API_VERSIONS)
    when(requestHeader.apiVersion()).thenReturn(0.toShort)

    val context = new RequestContext(requestHeader, "0", mock(classOf[InetAddress]), new KafkaPrincipal("", ""),
      new ListenerName(""), SecurityProtocol.PLAINTEXT, mock(classOf[ClientInformation]), false)
    new Request(0, context, time.nanoseconds(), mock(classOf[MemoryPool]), ByteBuffer.allocate(0), metrics)
  }

  @Test
  def testMetricsForMultipleRequestPools(): Unit = {
    val time = new MockTime()
    val metricsBroker = mock(classOf[RequestChannelMetrics])
    val metricsController = mock(classOf[RequestChannelMetrics])
    val requestChannelBroker = new RequestChannel(10, time, metricsBroker)
    val requestChannelController = new RequestChannel(10, time, metricsController)
    val apiHandler = mock(classOf[ApiRequestHandler])

    // Create a factory for this test
    val factory = new KafkaRequestHandlerPoolFactory()
    
    // Create broker pool with 4 threads
    val brokerPool = factory.createPool(
      0,
      requestChannelBroker,
      apiHandler,
      time,
      4,
      "broker"
    )

    // Verify global counter is updated
    assertEquals(4, factory.aggregateThreadCount, "global counter should be 4 after broker pool")

    // Create controller pool with 4 threads
    val controllerPool = factory.createPool(
      0,
      requestChannelController,
      apiHandler,
      time,
      4,
      "controller"
    )

    // Verify global counter is updated to sum of both pools
    assertEquals(8, factory.aggregateThreadCount, "global counter should be 8 after both pools")

    // Test pool resizing
    // Shrink broker pool from 4 to 2 threads
    brokerPool.resizeThreadPool(2)
    assertEquals(2, brokerPool.threadPoolSize.get)
    assertEquals(4, controllerPool.threadPoolSize.get)
    assertEquals(6, factory.aggregateThreadCount)

    // Expand controller pool from 4 to 6 threads
    controllerPool.resizeThreadPool(6)
    assertEquals(2, brokerPool.threadPoolSize.get)
    assertEquals(6, controllerPool.threadPoolSize.get)
    assertEquals(8, factory.aggregateThreadCount)
  }
}
