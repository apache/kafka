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
package integration.kafka.api

import kafka.api.{IntegrationTestHarness, SaslSetup}
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.security.oauthbearer.{OAuthBearerLoginCallbackHandler, OAuthBearerLoginModule, OAuthBearerValidatorCallbackHandler}
import org.apache.kafka.common.utils.Utils
import org.junit.jupiter.api.{AfterEach, BeforeEach, TestInfo}

import java.io.File
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption
import java.security.PrivateKey
import java.util
import java.util.{Base64, Properties}

/**
 * Base class for the OAuth client integration tests. It starts brokers that validate OAUTHBEARER tokens issued by
 * an authorization server, and builds the matching client configs. Subclasses provide the authorization server.
 */
abstract class AbstractClientOAuthIntegrationTest extends IntegrationTestHarness with SaslSetup {

  override val brokerCount = 3

  override protected def securityProtocol = SecurityProtocol.SASL_PLAINTEXT
  override protected val serverSaslProperties = Some(kafkaServerSaslProperties(kafkaServerSaslMechanisms, kafkaClientSaslMechanism))
  override protected val clientSaslProperties = Some(kafkaClientSaslProperties(kafkaClientSaslMechanism))

  protected def kafkaClientSaslMechanism = "OAUTHBEARER"
  protected def kafkaServerSaslMechanisms = List(kafkaClientSaslMechanism)

  protected def issuerUrl: String
  protected def tokenEndpointUrl: String
  protected def jwksUrl: String
  protected def brokerAudience: String
  protected def privateKey: PrivateKey
  protected def clientCredentialsClientId: String
  protected def clientCredentialsClientSecret: String

  // Called before the brokers are configured, so the endpoint urls above must be available afterwards.
  protected def startOAuthServer(): Unit = {}
  protected def stopOAuthServer(): Unit = {}

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    startOAuthServer()
    System.setProperty(BrokerSecurityConfigs.ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG, s"$tokenEndpointUrl,$jwksUrl")

    val listenerNamePrefix = s"listener.name.${listenerName.value().toLowerCase}"

    serverConfig.setProperty(s"$listenerNamePrefix.oauthbearer.${SaslConfigs.SASL_JAAS_CONFIG}", s"${classOf[OAuthBearerLoginModule].getName} required ;")
    serverConfig.setProperty(s"$listenerNamePrefix.oauthbearer.${SaslConfigs.SASL_OAUTHBEARER_EXPECTED_AUDIENCE}", brokerAudience)
    serverConfig.setProperty(s"$listenerNamePrefix.oauthbearer.${SaslConfigs.SASL_OAUTHBEARER_EXPECTED_ISSUER}", issuerUrl)
    serverConfig.setProperty(s"$listenerNamePrefix.oauthbearer.${SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_URL}", jwksUrl)
    serverConfig.setProperty(s"$listenerNamePrefix.oauthbearer.${BrokerSecurityConfigs.SASL_SERVER_CALLBACK_HANDLER_CLASS_CONFIG}", classOf[OAuthBearerValidatorCallbackHandler].getName)

    // create static config including client login context with credentials for JaasTestUtils 'client2'
    startSasl(jaasSections(kafkaServerSaslMechanisms, Option(kafkaClientSaslMechanism)))

    // The superuser needs the configuration in setUp because it's used to create resources before the individual
    // test methods are invoked.
    superuserClientConfig.putAll(defaultClientCredentialsConfigs())

    super.setUp(testInfo)
  }

  @AfterEach
  override def tearDown(): Unit = {
    stopOAuthServer()

    closeSasl()
    super.tearDown()

    System.clearProperty(BrokerSecurityConfigs.ALLOWED_SASL_OAUTHBEARER_FILES_CONFIG)
    System.clearProperty(BrokerSecurityConfigs.ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG)
  }

  def defaultOAuthConfigs(): Properties = {
    val configs = new Properties()
    configs.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol.name)
    configs.put(SaslConfigs.SASL_JAAS_CONFIG, jaasClientLoginModule(kafkaClientSaslMechanism))
    configs.put(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS, classOf[OAuthBearerLoginCallbackHandler].getName)
    configs.put(SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, tokenEndpointUrl)
    configs
  }

  def defaultClientCredentialsConfigs(): Properties = {
    val configs = defaultOAuthConfigs()
    configs.put(SaslConfigs.SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_ID, clientCredentialsClientId)
    configs.put(SaslConfigs.SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_SECRET, clientCredentialsClientSecret)
    configs
  }

  def generatePrivateKeyFile(): File = {
    val file = File.createTempFile("private-", ".key")
    val bytes = Base64.getEncoder.encode(privateKey.getEncoded)
    var channel: FileChannel = null

    try {
      channel = FileChannel.open(file.toPath, util.EnumSet.of(StandardOpenOption.WRITE))
      Utils.writeFully(channel, ByteBuffer.wrap(bytes))
    } finally {
      channel.close()
    }

    file
  }
}
