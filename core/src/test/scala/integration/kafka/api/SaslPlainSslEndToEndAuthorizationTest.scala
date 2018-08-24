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

import java.security.AccessController
import javax.security.auth.callback._
import javax.security.auth.Subject
import javax.security.auth.login.AppConfigurationEntry

import kafka.server.KafkaConfig
import kafka.utils.{CoreUtils, TestUtils, ZkUtils}
import kafka.utils.JaasTestUtils._
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.common.security.auth._
import org.apache.kafka.common.security.plain.PlainAuthenticateCallback
import org.junit.Test

object SaslPlainSslEndToEndAuthorizationTest {

  class TestPrincipalBuilder extends KafkaPrincipalBuilder {

    override def build(context: AuthenticationContext): KafkaPrincipal = {
      context match {
        case ctx: SaslAuthenticationContext =>
          ctx.server.getAuthorizationID match {
            case KafkaPlainAdmin =>
              new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin")
            case KafkaPlainUser =>
              new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "user")
            case _ =>
              KafkaPrincipal.ANONYMOUS
          }
      }
    }
  }

  object Credentials {
    val allUsers = Map(KafkaPlainUser -> "user1-password",
      KafkaPlainUser2 -> KafkaPlainPassword2,
      KafkaPlainAdmin -> "broker-password")
  }

  class TestServerCallbackHandler extends AuthenticateCallbackHandler {
    def configure(configs: java.util.Map[String, _], saslMechanism: String, jaasConfigEntries: java.util.List[AppConfigurationEntry]) {}
    def handle(callbacks: Array[Callback]) {
      var username: String = null
      for (callback <- callbacks) {
        if (callback.isInstanceOf[NameCallback])
          username = callback.asInstanceOf[NameCallback].getDefaultName
        else if (callback.isInstanceOf[PlainAuthenticateCallback]) {
          val plainCallback = callback.asInstanceOf[PlainAuthenticateCallback]
          plainCallback.authenticated(Credentials.allUsers(username) == new String(plainCallback.password))
        } else
          throw new UnsupportedCallbackException(callback)
      }
    }
    def close() {}
  }

  class TestClientCallbackHandler extends AuthenticateCallbackHandler {
    def configure(configs: java.util.Map[String, _], saslMechanism: String, jaasConfigEntries: java.util.List[AppConfigurationEntry]) {}
    def handle(callbacks: Array[Callback]) {
      val subject = Subject.getSubject(AccessController.getContext())
      val username = subject.getPublicCredentials(classOf[String]).iterator().next()
      for (callback <- callbacks) {
        if (callback.isInstanceOf[NameCallback])
          callback.asInstanceOf[NameCallback].setName(username)
        else if (callback.isInstanceOf[PasswordCallback]) {
          if (username == KafkaPlainUser || username == KafkaPlainAdmin)
            callback.asInstanceOf[PasswordCallback].setPassword(Credentials.allUsers(username).toCharArray)
        } else
          throw new UnsupportedCallbackException(callback)
      }
    }
    def close() {}
  }
}


// This test uses SASL callback handler overrides for server connections of Kafka broker
// and client connections of Kafka producers and consumers. Client connections from Kafka brokers
// used for inter-broker communication also use custom callback handlers. The second client used in
// the multi-user test SaslEndToEndAuthorizationTest#testTwoConsumersWithDifferentSaslCredentials uses
// static JAAS configuration with default callback handlers to test those code paths as well.
class SaslPlainSslEndToEndAuthorizationTest extends SaslEndToEndAuthorizationTest {
  import SaslPlainSslEndToEndAuthorizationTest._

  this.serverConfig.setProperty(BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG, classOf[TestPrincipalBuilder].getName)
  this.serverConfig.put(KafkaConfig.SaslClientCallbackHandlerClassProp, classOf[TestClientCallbackHandler].getName)
  val mechanismPrefix = ListenerName.forSecurityProtocol(SecurityProtocol.SASL_SSL).saslMechanismConfigPrefix("PLAIN")
  this.serverConfig.put(s"$mechanismPrefix${KafkaConfig.SaslServerCallbackHandlerClassProp}", classOf[TestServerCallbackHandler].getName)
  this.producerConfig.put(SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS, classOf[TestClientCallbackHandler].getName)
  this.consumerConfig.put(SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS, classOf[TestClientCallbackHandler].getName)
  private val plainLogin = s"org.apache.kafka.common.security.plain.PlainLoginModule username=$KafkaPlainUser required;"
  this.producerConfig.put(SaslConfigs.SASL_JAAS_CONFIG, plainLogin)
  this.consumerConfig.put(SaslConfigs.SASL_JAAS_CONFIG, plainLogin)

  override protected def kafkaClientSaslMechanism = "PLAIN"
  override protected def kafkaServerSaslMechanisms = List("PLAIN")

  override val clientPrincipal = "user"
  override val kafkaPrincipal = "admin"

  override def jaasSections(kafkaServerSaslMechanisms: Seq[String],
                            kafkaClientSaslMechanism: Option[String],
                            mode: SaslSetupMode,
                            kafkaServerEntryName: String): Seq[JaasSection] = {
    val brokerLogin = new PlainLoginModule(KafkaPlainAdmin, "") // Password provided by callback handler
    val clientLogin = new PlainLoginModule(KafkaPlainUser2, KafkaPlainPassword2)
    Seq(JaasSection(kafkaServerEntryName, Seq(brokerLogin)),
      JaasSection(KafkaClientContextName, Seq(clientLogin))) ++ zkSections
  }

  /**
   * Checks that secure paths created by broker and acl paths created by AclCommand
   * have expected ACLs.
   */
  @Test
  def testAcls() {
    val zkUtils = ZkUtils(zkConnect, zkSessionTimeout, zkConnectionTimeout, zkAclsEnabled.getOrElse(JaasUtils.isZkSecurityEnabled))
    TestUtils.verifySecureZkAcls(zkUtils, 1)
    CoreUtils.swallow(zkUtils.close(), this)
  }
}
