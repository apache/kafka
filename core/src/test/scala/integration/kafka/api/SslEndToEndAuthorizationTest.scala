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

package kafka.api

import org.apache.kafka.common.config.internals.BrokerSecurityConfigs
import org.apache.kafka.common.security.auth._
import org.junit.Before

object SslEndToEndAuthorizationTest {
  class TestPrincipalBuilder extends KafkaPrincipalBuilder {
    private val Pattern = "O=A (.*?),CN=localhost".r

    override def build(context: AuthenticationContext): KafkaPrincipal = {
      context match {
        case ctx: SslAuthenticationContext =>
          ctx.session.getPeerPrincipal.getName match {
            case Pattern(name) =>
              new KafkaPrincipal(KafkaPrincipal.USER_TYPE, name)
            case _ =>
              KafkaPrincipal.ANONYMOUS
          }
      }
    }
  }
}

class SslEndToEndAuthorizationTest extends EndToEndAuthorizationTest {
  import kafka.api.SslEndToEndAuthorizationTest.TestPrincipalBuilder

  override protected def securityProtocol = SecurityProtocol.SSL
  this.serverConfig.setProperty(BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG, "required")
  this.serverConfig.setProperty(BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG, classOf[TestPrincipalBuilder].getName)
  override val clientPrincipal = "client"
  override val kafkaPrincipal = "server"

  @Before
  override def setUp() {
    startSasl(jaasSections(List.empty, None, ZkSasl))
    super.setUp()
  }

}
