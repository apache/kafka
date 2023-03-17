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
import org.apache.kafka.clients.{ClientResponse, ManualMetadataUpdater, NetworkClient}
import org.apache.kafka.common.Node
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.AbstractResponse
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.Time
import org.junit.jupiter.api.{Assertions, Test}
import org.mockito.Mockito

import java.util.Collections

class BrokerToControllerChannelManagerImplTest {

  @Test
  def testInflightRequestsAreKeptWhenResetNetworkClient(): Unit = {
    val node = new Node(
      1,
      "127.0.0.1",
      22,
    )
    val client = Mockito.mock(classOf[NetworkClient])
    val thread = new BrokerToControllerRequestThread(
      client,
      true,
      _ => client,
      new ManualMetadataUpdater(),
      () => ControllerInformation(Some(node), ListenerName.normalised("PLAINTEXT"),
        SecurityProtocol.PLAINTEXT, "", true),
      Mockito.mock(classOf[KafkaConfig]),
      Time.SYSTEM,
      "test",
      1000
    )
    val response = Mockito.mock(classOf[ClientResponse])
    val body = Mockito.mock(classOf[AbstractResponse])
    Mockito.when(body.errorCounts()).thenReturn(Collections.singletonMap(Errors.NOT_CONTROLLER, 1))
    Mockito.when(response.responseBody()).thenReturn(body)
    // change the controller node
    thread.updateControllerAddress(new Node(300, "h", 22))
    // the previous controller node is NOT existent so it should NOT reset current controller
    thread.handleResponse(node, Mockito.mock(classOf[BrokerToControllerQueueItem]))(response)
    Assertions.assertNotEquals(Option.empty, thread.activeControllerAddress())
  }
}