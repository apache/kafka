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

import java.util.Properties

import kafka.utils.TestUtils
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs
import org.apache.kafka.common.network.Mode
import org.apache.kafka.common.security.auth._
import org.apache.kafka.common.security.authenticator.DefaultKafkaPrincipalBuilder
import org.apache.kafka.common.utils.Java
import org.junit.jupiter.api.{BeforeEach, TestInfo}

object SslEndToEndAuthorizationTest {
  class TestPrincipalBuilder extends DefaultKafkaPrincipalBuilder(null, null) {
    private val Pattern = "O=A (.*?),CN=(.*?)".r

    // Use full DN as client principal to test special characters in principal
    // Use field from DN as server principal to test custom PrincipalBuilder
    override def build(context: AuthenticationContext): KafkaPrincipal = {
      val peerPrincipal = context.asInstanceOf[SslAuthenticationContext].session.getPeerPrincipal.getName
      peerPrincipal match {
        case Pattern(name, _) =>
          val principal = if (name == "server") name else peerPrincipal
          new KafkaPrincipal(KafkaPrincipal.USER_TYPE, principal)
        case _ =>
          KafkaPrincipal.ANONYMOUS
      }
    }
  }
}

class SslEndToEndAuthorizationTest extends EndToEndAuthorizationTest {

  import kafka.api.SslEndToEndAuthorizationTest.TestPrincipalBuilder

  override protected def securityProtocol = SecurityProtocol.SSL
  // Since there are other E2E tests that enable SSL, running this test with TLSv1.3 if supported
  private  val tlsProtocol = if (Java.IS_JAVA11_COMPATIBLE) "TLSv1.3" else "TLSv1.2"

  this.serverConfig.setProperty(BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG, "required")
  this.serverConfig.setProperty(BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG, classOf[TestPrincipalBuilder].getName)
  this.serverConfig.setProperty(SslConfigs.SSL_PROTOCOL_CONFIG, tlsProtocol)
  this.serverConfig.setProperty(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, tlsProtocol)
  // Escaped characters in DN attribute values: from http://www.ietf.org/rfc/rfc2253.txt
  // - a space or "#" character occurring at the beginning of the string
  // - a space character occurring at the end of the string
  // - one of the characters ",", "+", """, "\", "<", ">" or ";"
  //
  // Leading and trailing spaces in Kafka principal dont work with ACLs, but we can workaround by using
  // a PrincipalBuilder that removes/replaces them.
  private val clientCn = """\#A client with special chars in CN : (\, \+ \" \\ \< \> \; ')"""
  override val clientPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, s"O=A client,CN=$clientCn")
  override val kafkaPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "server")
  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    startSasl(jaasSections(List.empty, None, ZkSasl))
    super.setUp(testInfo)
  }

  override def clientSecurityProps(certAlias: String): Properties = {
    val props = TestUtils.securityConfigs(Mode.CLIENT, securityProtocol, trustStoreFile,
      certAlias, clientCn, clientSaslProperties, tlsProtocol)
    props.remove(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG)
    props
  }
}
