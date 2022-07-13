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
package kafka.api

import org.apache.kafka.common.config.internals.BrokerSecurityConfigs
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.auth._
import org.apache.kafka.common.security.authenticator.DefaultKafkaPrincipalBuilder
import org.junit.jupiter.api.{BeforeEach, Test, TestInfo}
import org.junit.jupiter.api.Assertions._
import org.apache.kafka.common.errors.TopicAuthorizationException

// This test case uses a separate listener for client and inter-broker communication, from
// which we derive corresponding principals
object PlaintextEndToEndAuthorizationTest {
  @volatile
  private var clientListenerName = None: Option[String]
  @volatile
  private var serverListenerName = None: Option[String]
  class TestClientPrincipalBuilder extends DefaultKafkaPrincipalBuilder(null, null) {
    override def build(context: AuthenticationContext): KafkaPrincipal = {
      clientListenerName = Some(context.listenerName)
      context match {
        case ctx: PlaintextAuthenticationContext if ctx.clientAddress != null =>
          new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "client")
        case _ =>
          KafkaPrincipal.ANONYMOUS
      }
    }
  }

  class TestServerPrincipalBuilder extends DefaultKafkaPrincipalBuilder(null, null) {
    override def build(context: AuthenticationContext): KafkaPrincipal = {
      serverListenerName = Some(context.listenerName)
      context match {
        case ctx: PlaintextAuthenticationContext =>
          new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "server")
        case _ =>
          KafkaPrincipal.ANONYMOUS
      }
    }
  }
}

class PlaintextEndToEndAuthorizationTest extends EndToEndAuthorizationTest {
  import PlaintextEndToEndAuthorizationTest.{TestClientPrincipalBuilder, TestServerPrincipalBuilder}

  override protected def securityProtocol = SecurityProtocol.PLAINTEXT
  override protected def listenerName: ListenerName = new ListenerName("CLIENT")
  override protected def interBrokerListenerName: ListenerName = new ListenerName("SERVER")

  this.serverConfig.setProperty("listener.name.client." + BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG,
    classOf[TestClientPrincipalBuilder].getName)
  this.serverConfig.setProperty("listener.name.server." + BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG,
    classOf[TestServerPrincipalBuilder].getName)
  override val clientPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "client")
  override val kafkaPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "server")

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    startSasl(jaasSections(List.empty, None, ZkSasl))
    super.setUp(testInfo)
  }

  @Test
  def testListenerName(): Unit = {
    // To check the client listener name, establish a session on the server by sending any request eg sendRecords
    val producer = createProducer()
    assertThrows(classOf[TopicAuthorizationException], () => sendRecords(producer, numRecords = 1, tp))

    assertEquals(Some("CLIENT"), PlaintextEndToEndAuthorizationTest.clientListenerName)
    assertEquals(Some("SERVER"), PlaintextEndToEndAuthorizationTest.serverListenerName)
  }

}
