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
import kafka.network.RequestChannel.EndThrottlingResponse
import kafka.network.RequestChannel.Session
import kafka.network.RequestChannel.StartThrottlingResponse
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.memory.MemoryPool
import org.apache.kafka.common.metrics.MetricConfig
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.ClientInformation
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.requests.{AbstractRequest, FetchRequest, RequestContext, RequestHeader, RequestTestUtils}
import org.apache.kafka.common.requests.FetchRequest.PartitionData
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.MockTime
import org.easymock.EasyMock
import org.junit.After

class BaseClientQuotaManagerTest {
  protected val time = new MockTime
  protected var numCallbacks: Int = 0
  protected val metrics = new Metrics(new MetricConfig(), Collections.emptyList(), time)

  @After
  def tearDown(): Unit = {
    metrics.close()
  }

  protected def callback(response: RequestChannel.Response): Unit = {
    // Count how many times this callback is called for notifyThrottlingDone().
    (response: @unchecked) match {
      case _: StartThrottlingResponse =>
      case _: EndThrottlingResponse => numCallbacks += 1
    }
  }

  protected def buildRequest[T <: AbstractRequest](builder: AbstractRequest.Builder[T],
                                                   listenerName: ListenerName = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)): (T, RequestChannel.Request) = {

    val request = builder.build()
    val buffer = RequestTestUtils.serializeRequestWithHeader(
      new RequestHeader(builder.apiKey, request.version, "", 0), request)
    val requestChannelMetrics: RequestChannel.Metrics = EasyMock.createNiceMock(classOf[RequestChannel.Metrics])

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
                         channelThrottlingCallback: RequestChannel.Response => Unit): Unit = {
    val (_, request) = buildRequest(FetchRequest.Builder.forConsumer(0, 1000, new util.HashMap[TopicPartition, PartitionData]))
    quotaManager.throttle(request, throttleTimeMs, channelThrottlingCallback)
  }
}
