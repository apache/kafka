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
import org.apache.kafka.common.utils.MockTime
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, when}

import java.net.InetAddress
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger

class KafkaRequestHandlerTest {

  @Test
  def testCallbackTiming(): Unit = {
    val time = new MockTime()
    val startTime = time.nanoseconds()
    val metrics = new RequestChannel.Metrics(None)
    val requestChannel = new RequestChannel(10, "", time, metrics)
    val apiHandler = mock(classOf[ApiRequestHandler])

    // Make unsupported API versions request to avoid having to parse a real request
    val requestHeader = mock(classOf[RequestHeader])
    when(requestHeader.apiKey()).thenReturn(ApiKeys.API_VERSIONS)
    when(requestHeader.apiVersion()).thenReturn(0.toShort)

    val context = new RequestContext(requestHeader, "0", mock(classOf[InetAddress]), new KafkaPrincipal("", ""),
      new ListenerName(""), SecurityProtocol.PLAINTEXT, mock(classOf[ClientInformation]), false)
    val request = new RequestChannel.Request(0, context, time.nanoseconds(),
      mock(classOf[MemoryPool]), mock(classOf[ByteBuffer]), metrics)

    val handler = new KafkaRequestHandler(0, 0, mock(classOf[Meter]), new AtomicInteger(1), requestChannel, apiHandler, time)

    requestChannel.sendRequest(request)

    def callback(ms: Int): Unit = {
      time.sleep(ms)
      handler.stop()
    }

    when(apiHandler.handle(ArgumentMatchers.eq(request), any())).thenAnswer { _ =>
      time.sleep(2)
      KafkaRequestHandler.wrap(callback(_: Int))(1)
      request.apiLocalCompleteTimeNanos = time.nanoseconds
    }

    handler.run()

    assertEquals(startTime, request.requestDequeueTimeNanos)
    assertEquals(startTime + 2000000, request.apiLocalCompleteTimeNanos)
    assertEquals(Some(startTime + 2000000), request.callbackRequestDequeueTimeNanos)
    assertEquals(Some(startTime + 3000000), request.callbackRequestCompleteTimeNanos)
  }
}
