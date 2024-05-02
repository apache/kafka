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
package kafka.api

import kafka.utils.JaasTestUtils._
import kafka.utils.TestUtils
import kafka.utils.TestUtils.isAclSecure
import kafka.zk.ZkData
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs
import org.apache.kafka.common.network.Mode
import org.apache.kafka.common.security.auth._
import org.apache.kafka.common.security.authenticator.DefaultKafkaPrincipalBuilder
import org.apache.kafka.common.security.plain.PlainAuthenticateCallback
import org.apache.kafka.server.config.KafkaSecurityConfigs
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test

import java.security.AccessController
import java.util.Properties
import javax.security.auth.Subject
import javax.security.auth.callback._
import javax.security.auth.login.AppConfigurationEntry
import scala.collection.Seq

object SaslPlainSslEndToEndAuthorizationTest {

  val controllerPrincipalName = "admin"

  class TestPrincipalBuilder extends DefaultKafkaPrincipalBuilder(null, null) {
    override def build(context: AuthenticationContext): KafkaPrincipal = {
      val saslContext = context.asInstanceOf[SaslAuthenticationContext]

      // Verify that peer principal can be obtained from the SSLSession provided in the context
      // since we have enabled TLS mutual authentication for the listener
      val sslPrincipal = saslContext.sslSession.get.getPeerPrincipal.getName
      assertTrue(sslPrincipal.endsWith(s"CN=${TestUtils.SslCertificateCn}"), s"Unexpected SSL principal $sslPrincipal")

      saslContext.server.getAuthorizationID match {
        case KafkaPlainAdmin =>
          new KafkaPrincipal(KafkaPrincipal.USER_TYPE, controllerPrincipalName)
        case KafkaPlainUser =>
          new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "user")
        case _ =>
          KafkaPrincipal.ANONYMOUS
      }
    }
  }

  object Credentials {
    val allUsers = Map(KafkaPlainUser -> "user1-password",
      KafkaPlainUser2 -> KafkaPlainPassword2,
      KafkaPlainAdmin -> "broker-password")
  }

  class TestServerCallbackHandler extends AuthenticateCallbackHandler {
    def configure(configs: java.util.Map[String, _], saslMechanism: String, jaasConfigEntries: java.util.List[AppConfigurationEntry]): Unit = {}
    def handle(callbacks: Array[Callback]): Unit = {
      var username: String = null
      for (callback <- callbacks) {
        callback match {
          case nameCallback: NameCallback => username = nameCallback.getDefaultName
          case plainCallback: PlainAuthenticateCallback =>
            plainCallback.authenticated(Credentials.allUsers(username) == new String(plainCallback.password))
          case _ => throw new UnsupportedCallbackException(callback)
        }
      }
    }
    def close(): Unit = {}
  }

  class TestClientCallbackHandler extends AuthenticateCallbackHandler {
    def configure(configs: java.util.Map[String, _], saslMechanism: String, jaasConfigEntries: java.util.List[AppConfigurationEntry]): Unit = {}
    def handle(callbacks: Array[Callback]): Unit = {
      val subject = Subject.getSubject(AccessController.getContext)
      val username = subject.getPublicCredentials(classOf[String]).iterator().next()
      for (callback <- callbacks) {
        callback match {
          case nameCallback: NameCallback => nameCallback.setName(username)
          case passwordCallback: PasswordCallback =>
            if (username == KafkaPlainUser || username == KafkaPlainAdmin)
              passwordCallback.setPassword(Credentials.allUsers(username).toCharArray)
          case _ => throw new UnsupportedCallbackException(callback)
        }
      }
    }
    def close(): Unit = {}
  }
}


// This test uses SASL callback handler overrides for server connections of Kafka broker
// and client connections of Kafka producers and consumers. Client connections from Kafka brokers
// used for inter-broker communication also use custom callback handlers. The second client used in
// the multi-user test SaslEndToEndAuthorizationTest#testTwoConsumersWithDifferentSaslCredentials uses
// static JAAS configuration with default callback handlers to test those code paths as well.
class SaslPlainSslEndToEndAuthorizationTest extends SaslEndToEndAuthorizationTest {
  import SaslPlainSslEndToEndAuthorizationTest._

  this.serverConfig.setProperty(s"${listenerName.configPrefix}${KafkaSecurityConfigs.SSL_CLIENT_AUTH_CONFIG}", "required")
  this.serverConfig.setProperty(BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG, classOf[TestPrincipalBuilder].getName)
  this.serverConfig.put(KafkaSecurityConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS_CONFIG, classOf[TestClientCallbackHandler].getName)
  val mechanismPrefix = listenerName.saslMechanismConfigPrefix("PLAIN")
  this.serverConfig.put(s"$mechanismPrefix${KafkaSecurityConfigs.SASL_SERVER_CALLBACK_HANDLER_CLASS_CONFIG}", classOf[TestServerCallbackHandler].getName)
  this.producerConfig.put(SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS, classOf[TestClientCallbackHandler].getName)
  this.consumerConfig.put(SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS, classOf[TestClientCallbackHandler].getName)
  this.adminClientConfig.put(SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS, classOf[TestClientCallbackHandler].getName)
  this.superuserClientConfig.put(SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS, classOf[TestClientCallbackHandler].getName)

  override protected def kafkaClientSaslMechanism = "PLAIN"
  override protected def kafkaServerSaslMechanisms = List("PLAIN")

  override val clientPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "user")
  override val kafkaPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, controllerPrincipalName)

  override def jaasSections(kafkaServerSaslMechanisms: Seq[String],
                            kafkaClientSaslMechanism: Option[String],
                            mode: SaslSetupMode,
                            kafkaServerEntryName: String): Seq[JaasSection] = {
    val brokerLogin = PlainLoginModule(KafkaPlainAdmin, "") // Password provided by callback handler
    val clientLogin = PlainLoginModule(KafkaPlainUser2, KafkaPlainPassword2)
    Seq(JaasSection(kafkaServerEntryName, Seq(brokerLogin)),
      JaasSection(KafkaClientContextName, Seq(clientLogin))) ++ zkSections
  }

  // Generate SSL certificates for clients since we are enabling TLS mutual authentication
  // in this test for the SASL_SSL listener.
  override def clientSecurityProps(certAlias: String): Properties = {
    TestUtils.securityConfigs(Mode.CLIENT, securityProtocol, trustStoreFile, certAlias, TestUtils.SslCertificateCn,
      clientSaslProperties, needsClientCert = Some(true))
  }

  /**
   * Checks that secure paths created by broker and acl paths created by AclCommand
   * have expected ACLs.
   */
  @Test
  def testAcls(): Unit = {
    TestUtils.secureZkPaths(zkClient).foreach(path => {
      if (zkClient.pathExists(path)) {
        val sensitive = ZkData.sensitivePath(path)
        // usersWithAccess have ALL access to path. For paths that are
        // not sensitive, world has READ access.
        val aclCount = if (sensitive) 1 else 1 + 1
        val acls = zkClient.getAcl(path)
        assertEquals(aclCount, acls.size, s"Invalid ACLs for $path $acls")
        acls.foreach(acl => isAclSecure(acl, sensitive))
      }
    })
  }
}
