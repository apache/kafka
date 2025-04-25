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
import kafka.utils.TestInfoUtils
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.config.SaslConfigs
import org.junit.jupiter.api.{AfterEach, BeforeEach, TestInfo}

import java.util.Properties
import no.nav.security.mock.oauth2.MockOAuth2Server
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.security.oauthbearer.{OAuthBearerLoginCallbackHandler, OAuthBearerLoginModule, OAuthBearerValidatorCallbackHandler}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource

/**
 * Integration tests for the consumer that cover basic usage as well as coordinator failure
 */
class ClientOAuthIntegrationTest extends IntegrationTestHarness with SaslSetup {

  override val brokerCount = 3

  override protected def securityProtocol = SecurityProtocol.SASL_PLAINTEXT
  override protected val serverSaslProperties = Some(kafkaServerSaslProperties(kafkaServerSaslMechanisms, kafkaClientSaslMechanism))
  override protected val clientSaslProperties = Some(kafkaClientSaslProperties(kafkaClientSaslMechanism))

  protected def kafkaClientSaslMechanism = "OAUTHBEARER"
  protected def kafkaServerSaslMechanisms = List(kafkaClientSaslMechanism)

  val issuerId = "default"
  var mockOAuthServer: Option[MockOAuth2Server] = None

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    mockOAuthServer = Option(new MockOAuth2Server())
    mockOAuthServer.get.start()
    val tokenEndpointUrl = mockOAuthServer.get.tokenEndpointUrl(issuerId).url().toString
    val jwksUrl = mockOAuthServer.get.jwksUrl(issuerId).url().toString
    System.setProperty("org.apache.kafka.sasl.oauthbearer.allowed.urls", s"$tokenEndpointUrl,$jwksUrl")

    val listenerNamePrefix = s"listener.name.${listenerName.value().toLowerCase}"

    serverConfig.setProperty(s"$listenerNamePrefix.oauthbearer.${SaslConfigs.SASL_JAAS_CONFIG}", s"${classOf[OAuthBearerLoginModule].getName} required ;")
    serverConfig.setProperty(s"$listenerNamePrefix.oauthbearer.${SaslConfigs.SASL_OAUTHBEARER_EXPECTED_AUDIENCE}", issuerId)
    serverConfig.setProperty(s"$listenerNamePrefix.oauthbearer.${SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_URL}", jwksUrl)
    serverConfig.setProperty(s"$listenerNamePrefix.oauthbearer.${BrokerSecurityConfigs.SASL_SERVER_CALLBACK_HANDLER_CLASS_CONFIG}", classOf[OAuthBearerValidatorCallbackHandler].getName)

    // create static config including client login context with credentials for JaasTestUtils 'client2'
    startSasl(jaasSections(kafkaServerSaslMechanisms, Option(kafkaClientSaslMechanism)))

    val clientSaslConfig = new Properties()
    clientSaslConfig.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol.name)
    clientSaslConfig.put(SaslConfigs.SASL_JAAS_CONFIG, jaasClientLoginModule(kafkaClientSaslMechanism))
    clientSaslConfig.put(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS, classOf[OAuthBearerLoginCallbackHandler].getName)
    clientSaslConfig.put(SaslConfigs.SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_ID, "test-client")
    clientSaslConfig.put(SaslConfigs.SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_SECRET, "test-secret")
    clientSaslConfig.put(SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, tokenEndpointUrl)

    producerConfig.putAll(clientSaslConfig)
    consumerConfig.putAll(clientSaslConfig)
    adminClientConfig.putAll(clientSaslConfig)
    superuserClientConfig.putAll(clientSaslConfig)

    super.setUp(testInfo)
  }

  @AfterEach
  override def tearDown(): Unit = {
    if (mockOAuthServer.isDefined)
      mockOAuthServer.get.shutdown()

    closeSasl()
    super.tearDown()
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testSimpleConnect(groupProtocol: String): Unit = {
    createProducer()
    createConsumer()
    createAdminClient()
  }
}
