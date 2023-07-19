/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.server

import java.net.InetAddress
import java.util
import java.util.Collections

import kafka.network.RequestChannel
import kafka.network.RequestChannel.Session
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.memory.MemoryPool
import org.apache.kafka.common.metrics.{MetricConfig, Metrics}
import org.apache.kafka.common.network.{ClientInformation, ListenerName}
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.FetchRequest.PartitionData
import org.apache.kafka.common.requests.{AbstractRequest, FetchRequest, RequestContext, RequestHeader}
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.apache.kafka.common.utils.MockTime
import org.junit.jupiter.api.AfterEach
import org.mockito.Mockito.mock

class BaseClientQuotaManagerTest {
  protected val time = new MockTime
  protected var numCallbacks: Int = 0
  protected val metrics = new Metrics(new MetricConfig(), Collections.emptyList(), time)

  @AfterEach
  def tearDown(): Unit = {
    metrics.close()
  }

  protected def callback: ThrottleCallback = new ThrottleCallback {
    override def startThrottling(): Unit = {}
    override def endThrottling(): Unit = {
      // Count how many times this callback is called for notifyThrottlingDone().
      numCallbacks += 1
    }
  }

  protected def buildRequest[T <: AbstractRequest](builder: AbstractRequest.Builder[T],
                                                   listenerName: ListenerName = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)): (T, RequestChannel.Request) = {

    val request = builder.build()
    val buffer = request.serializeWithHeader(new RequestHeader(builder.apiKey, request.version, "", 0))
    val requestChannelMetrics: RequestChannel.Metrics = mock(classOf[RequestChannel.Metrics])

    // read the header from the buffer first so that the body can be read next from the Request constructor
    val header = RequestHeader.parse(buffer)
    val context = new RequestContext(header, "1", InetAddress.getLocalHost, KafkaPrincipal.ANONYMOUS,
      listenerName, SecurityProtocol.PLAINTEXT, ClientInformation.EMPTY, false)
    (request, new RequestChannel.Request(processor = 1, context = context, startTimeNanos =  0, MemoryPool.NONE, buffer,
      requestChannelMetrics))
  }

  protected def buildSession(user: String): Session = {
    val principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, user)
    Session(principal, null)
  }

  protected def maybeRecord(quotaManager: ClientQuotaManager, user: String, clientId: String, value: Double): Int = {

    quotaManager.maybeRecordAndGetThrottleTimeMs(buildSession(user), clientId, value, time.milliseconds)
  }

  protected def throttle(quotaManager: ClientQuotaManager, user: String, clientId: String, throttleTimeMs: Int,
                         channelThrottlingCallback: ThrottleCallback): Unit = {
    val (_, request) = buildRequest(FetchRequest.Builder.forConsumer(ApiKeys.FETCH.latestVersion, 0, 1000, new util.HashMap[TopicPartition, PartitionData]))
    quotaManager.throttle(request, channelThrottlingCallback, throttleTimeMs)
  }
}
