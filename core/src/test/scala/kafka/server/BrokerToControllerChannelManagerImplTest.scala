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
package kafka.server
import org.apache.kafka.clients.{ClientRequest, ClientResponse, ManualMetadataUpdater, NetworkClient}
import org.apache.kafka.common.Node
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.requests.AbstractRequest
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.Time
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.{Assertions, Test}
import org.mockito.ArgumentMatchers.{any, refEq}
import org.mockito.Mockito

class BrokerToControllerChannelManagerImplTest {

  @Test
  def testInflightRequestsAreKeptWhenResetNetworkClient(): Unit = {
    var controllerNode = ControllerInformation(Some(new Node(
      1,
      "127.0.0.1",
      22,
    )), ListenerName.normalised("PLAINTEXT"),
      SecurityProtocol.PLAINTEXT, "", true)
    val provider: ControllerNodeProvider = () => controllerNode
    val config = Mockito.mock(classOf[KafkaConfig])
    val client = Mockito.mock(classOf[NetworkClient])
    val builder = Mockito.mock(classOf[AbstractRequest.Builder[AbstractRequest]])
    val request = Mockito.mock(classOf[AbstractRequest])
    val clientRequest = new ClientRequest("test", builder, 1, "1",
      System.currentTimeMillis(), true, 300000, _ => {})
    // count the sent requests
    var clientRequestCount = 0
    Mockito.when(client.newClientRequest(any(), refEq(builder), any(), any(), any(), any())).thenAnswer {
      _ =>
        clientRequestCount = clientRequestCount + 1
        clientRequest
    }
    Mockito.when(client.ready(any(), any())).thenReturn(true)
    val thread = new BrokerToControllerRequestThread(
      client,
      true,
      _ => client,
      new ManualMetadataUpdater(),
      provider,
      config,
      Time.SYSTEM,
      "test",
      1000
    )
    Mockito.when(builder.build()).thenReturn(request)
    thread.started = true
    thread.enqueue(BrokerToControllerQueueItem(System.currentTimeMillis(),
      builder, new ControllerRequestCompletionHandler(){
      override def onTimeout(): Unit = fail("request should not be timeout")
      override def onComplete(response: ClientResponse): Unit = fail("request should not be completed")
    }))

    // 1: set the active controller
    thread.doWork()
    Assertions.assertEquals(Option(new Node(
      1,
      "127.0.0.1",
      22,
    )), thread.activeControllerAddress())
    Assertions.assertEquals(1, thread.queueSize)

    // 2: create first client request
    thread.doWork()
    Assertions.assertEquals(0, thread.queueSize)
    Assertions.assertEquals(1, clientRequestCount)

    // migrate the controller from zk to kraft
    controllerNode = ControllerInformation(Some(new Node(
      2,
      "127.0.0.1",
      22,
    )), ListenerName.normalised("PLAINTEXT"),
      SecurityProtocol.PLAINTEXT, "", false)

    // 3: reset the network client, and re-create client request for all unsent requests
    thread.doWork()
    Assertions.assertEquals(Option(new Node(
      2,
      "127.0.0.1",
      22,
    )), thread.activeControllerAddress())
    Assertions.assertEquals(2, clientRequestCount)
  }
}
