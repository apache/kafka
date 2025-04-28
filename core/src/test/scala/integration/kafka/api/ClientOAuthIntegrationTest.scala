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
import org.apache.kafka.common.config.{ConfigException, SaslConfigs}
import org.junit.jupiter.api.{AfterEach, BeforeEach, TestInfo}

import java.util.{Collections, Properties}
import no.nav.security.mock.oauth2.MockOAuth2Server
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.security.oauthbearer.internals.secured.JwtBearerRequestGenerator
import org.apache.kafka.common.security.oauthbearer.{OAuthBearerLoginCallbackHandler, OAuthBearerLoginModule, OAuthBearerValidatorCallbackHandler}
import org.apache.kafka.test.TestUtils
import org.junit.jupiter.api.Assertions.{assertDoesNotThrow, assertThrows}
import org.junit.jupiter.api.function.Executable
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource

import javax.security.auth.login.LoginException

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
  var mockOAuthServer: MockOAuth2Server = _

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    mockOAuthServer = new MockOAuth2Server()
    mockOAuthServer.start()
    val tokenEndpointUrl = mockOAuthServer.tokenEndpointUrl(issuerId).url().toString
    val jwksUrl = mockOAuthServer.jwksUrl(issuerId).url().toString
    System.setProperty("org.apache.kafka.sasl.oauthbearer.allowed.urls", s"$tokenEndpointUrl,$jwksUrl")

    val listenerNamePrefix = s"listener.name.${listenerName.value().toLowerCase}"

    serverConfig.setProperty(s"$listenerNamePrefix.oauthbearer.${SaslConfigs.SASL_JAAS_CONFIG}", s"${classOf[OAuthBearerLoginModule].getName} required ;")
    serverConfig.setProperty(s"$listenerNamePrefix.oauthbearer.${SaslConfigs.SASL_OAUTHBEARER_EXPECTED_AUDIENCE}", issuerId)
    serverConfig.setProperty(s"$listenerNamePrefix.oauthbearer.${SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_URL}", jwksUrl)
    serverConfig.setProperty(s"$listenerNamePrefix.oauthbearer.${BrokerSecurityConfigs.SASL_SERVER_CALLBACK_HANDLER_CLASS_CONFIG}", classOf[OAuthBearerValidatorCallbackHandler].getName)

    // create static config including client login context with credentials for JaasTestUtils 'client2'
    startSasl(jaasSections(kafkaServerSaslMechanisms, Option(kafkaClientSaslMechanism)))

    // The superuser needs the configuration in setUp because it's used to create resources before the individual
    // test methods are invoked.
    superuserClientConfig.putAll(defaultOAuthClientConfigs())

    super.setUp(testInfo)
  }

  @AfterEach
  override def tearDown(): Unit = {
    if (mockOAuthServer != null)
      mockOAuthServer.shutdown()

    closeSasl()
    super.tearDown()
  }

  def defaultOAuthClientConfigs(): Properties = {
    val tokenEndpointUrl = mockOAuthServer.tokenEndpointUrl(issuerId).url().toString

    val configs = new Properties()
    configs.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol.name)
    configs.put(SaslConfigs.SASL_JAAS_CONFIG, jaasClientLoginModule(kafkaClientSaslMechanism))
    configs.put(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS, classOf[OAuthBearerLoginCallbackHandler].getName)
    configs.put(SaslConfigs.SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_ID, "test-client")
    configs.put(SaslConfigs.SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_SECRET, "test-secret")
    configs.put(SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, tokenEndpointUrl)
    configs
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testBasicClientCredentials(groupProtocol: String): Unit = {
    val configs = defaultOAuthClientConfigs()
    assertClientsSucceed(configs)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testBasicJwtBearer(groupProtocol: String): Unit = {
    val jwt = mockOAuthServer.issueToken(issuerId, "jdoe", "someaudience", Collections.singletonMap("scope", "test"))
    val assertionFile = TestUtils.tempFile(jwt.serialize())

    val configs = defaultOAuthClientConfigs()
    configs.put(SaslConfigs.SASL_OAUTHBEARER_GRANT_TYPE, JwtBearerRequestGenerator.GRANT_TYPE)
    configs.put(SaslConfigs.SASL_OAUTHBEARER_ASSERTION_FILE, assertionFile.getAbsolutePath)

    assertClientsSucceed(configs)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testJwtBearerWithMalformedAssertionFile(groupProtocol: String): Unit = {
    // Create the assertion file, but fill it with non-JWT garbage.
    val assertionFile = TestUtils.tempFile("CQEN*)Q#F)&)^#QNC")

    val configs = defaultOAuthClientConfigs()
    configs.put(SaslConfigs.SASL_OAUTHBEARER_GRANT_TYPE, JwtBearerRequestGenerator.GRANT_TYPE)
    configs.put(SaslConfigs.SASL_OAUTHBEARER_ASSERTION_FILE, assertionFile.getAbsolutePath)

    assertClientsThrowException(configs, classOf[LoginException], "invalid request: the assertion is not a jwt")
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testJwtBearerWithEmptyAssertionFile(groupProtocol: String): Unit = {
    // Create the assertion file, but leave it empty.
    val assertionFile = TestUtils.tempFile()

    val configs = defaultOAuthClientConfigs()
    configs.put(SaslConfigs.SASL_OAUTHBEARER_GRANT_TYPE, JwtBearerRequestGenerator.GRANT_TYPE)
    configs.put(SaslConfigs.SASL_OAUTHBEARER_ASSERTION_FILE, assertionFile.getAbsolutePath)

    assertClientsThrowException(configs, classOf[LoginException], "invalid request: missing or empty assertion parameter")
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testJwtBearerWithMissingAssertionFile(groupProtocol: String): Unit = {
    val missingFileName = "/this/does/not/exist.txt"

    val configs = defaultOAuthClientConfigs()
    configs.put(SaslConfigs.SASL_OAUTHBEARER_GRANT_TYPE, JwtBearerRequestGenerator.GRANT_TYPE)
    configs.put(SaslConfigs.SASL_OAUTHBEARER_ASSERTION_FILE, missingFileName)

    assertClientsThrowException(configs, classOf[ConfigException], s"contains a file ($missingFileName) that doesn't exist")
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testUnsupportedGrantType(groupProtocol: String): Unit = {
    val invalidGrantType = "this-is-an-invalid-grant-type"

    val configs = defaultOAuthClientConfigs()
    configs.put(SaslConfigs.SASL_OAUTHBEARER_GRANT_TYPE, invalidGrantType)
    configs.remove(SaslConfigs.SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_ID)
    configs.remove(SaslConfigs.SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_SECRET)

    assertClientsThrowException(configs, classOf[ConfigException], s"The grant type \"$invalidGrantType\" is not supported")
  }

  def assertClientsSucceed(configs: Properties): Unit = {
    assertDoesNotThrow(() => createProducer(configOverrides = configs))
    assertDoesNotThrow(() => createConsumer(configOverrides = configs))
    assertDoesNotThrow(() => createAdminClient(configOverrides = configs))
  }

  def assertClientsThrowException[T <: Throwable](configs: Properties,
                                                  expectedExceptionClass: Class[T],
                                                  messageSubstring: String): Unit = {
    assertThrowsException(() => createProducer(configOverrides = configs), expectedExceptionClass, messageSubstring)
    assertThrowsException(() => createConsumer(configOverrides = configs), expectedExceptionClass, messageSubstring)
    assertThrowsException(() => createAdminClient(configOverrides = configs), expectedExceptionClass, messageSubstring)
  }

  def assertThrowsException[T <: Throwable](executable: Executable,
                                            expectedExceptionClass: Class[T],
                                            expectedMessageSubstring: String): Unit = {
    val original = assertThrows(classOf[Throwable], executable)
    var cause = original

    if (expectedExceptionClass.isInstance(cause) && cause.getMessage.contains(expectedMessageSubstring))
      return

    while (cause.getCause != null) {
      cause = cause.getCause

      if (expectedExceptionClass.isInstance(cause) && cause.getMessage.contains(expectedMessageSubstring))
        return
    }

    throw original
  }
}
