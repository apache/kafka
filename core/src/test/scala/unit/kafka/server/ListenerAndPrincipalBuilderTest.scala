/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package kafka.server

import java.net.Socket
import java.util.Properties

import kafka.utils.TestUtils
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.types.Struct
import org.apache.kafka.common.requests._
import org.apache.kafka.common.security.auth.{AuthenticationContext, KafkaPrincipal, KafkaPrincipalBuilder}
import org.junit.Assert._
import org.junit.{Before, Test}

class ListenerAndPrincipalBuilderTest extends BaseRequestTest {

  override def numBrokers: Int = 1

  override def propertyOverrides(properties: Properties): Unit = {
    properties.put(KafkaConfig.PrincipalBuilderClassProp, classOf[ListenerAndPrincipalBuilderTest.TestPrincipalBuilder].getName)
    properties.put(KafkaConfig.ListenersProp, "PLAINTEXT://localhost:0,CUSTOM://localhost:0")
    properties.put(KafkaConfig.ListenerSecurityProtocolMapProp, "PLAINTEXT:PLAINTEXT,CUSTOM:PLAINTEXT")
  }

  @Before
  override def setUp() {
    super.setUp()

    TestUtils.retry(10000) {
      // controller to node connection using the default security.inter.broker.protocol
      assertEquals("PLAINTEXT", ListenerAndPrincipalBuilderTest.lastListenerName)
    }
  }

  @Test
  def testListenerNameFromAuthenticationContext() {
    val port = anySocketServer.boundPort(new ListenerName("CUSTOM"))
    val socket = new Socket("localhost", port)
    try {
      // using the simplest possible builder
      requestResponse(socket, "clientId", 0, new ListGroupsRequest.Builder())
    } finally {
      socket.close()
    }
    assertEquals("CUSTOM", ListenerAndPrincipalBuilderTest.lastListenerName)
  }

  private def requestResponse(socket: Socket, clientId: String, correlationId: Int, requestBuilder: AbstractRequest.Builder[_ <: AbstractRequest]): Struct = {
    val apiKey = requestBuilder.apiKey
    val request = requestBuilder.build()
    val header = new RequestHeader(apiKey, request.version, clientId, correlationId)
    val response = requestAndReceive(socket, request.serialize(header).array)
    val responseBuffer = skipResponseHeader(response)
    apiKey.parseResponse(request.version, responseBuffer)
  }

}

object ListenerAndPrincipalBuilderTest {
  var lastListenerName = ""

  class TestPrincipalBuilder extends KafkaPrincipalBuilder {
    override def build(context: AuthenticationContext): KafkaPrincipal = {
      lastListenerName = context.listenerName.value
      KafkaPrincipal.ANONYMOUS
    }
  }
}
