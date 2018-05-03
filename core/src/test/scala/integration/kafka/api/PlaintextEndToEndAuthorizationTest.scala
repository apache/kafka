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
import org.junit.Before

// This test case uses a separate listener for client and inter-broker communication, from
// which we derive corresponding principals
object PlaintextEndToEndAuthorizationTest {
  class TestClientPrincipalBuilder extends KafkaPrincipalBuilder {
    override def build(context: AuthenticationContext): KafkaPrincipal = {
      context match {
        case ctx: PlaintextAuthenticationContext if ctx.clientAddress != null =>
          new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "client")
        case _ =>
          KafkaPrincipal.ANONYMOUS
      }
    }
  }

  class TestServerPrincipalBuilder extends KafkaPrincipalBuilder {
    override def build(context: AuthenticationContext): KafkaPrincipal = {
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
  override val clientPrincipal = "client"
  override val kafkaPrincipal = "server"

  @Before
  override def setUp(): Unit = {
    startSasl(jaasSections(List.empty, None, ZkSasl))
    super.setUp()
  }

}
