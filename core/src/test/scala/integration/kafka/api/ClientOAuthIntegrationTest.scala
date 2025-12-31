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

import com.nimbusds.jose.jwk.RSAKey
import kafka.api.{IntegrationTestHarness, SaslSetup}
import kafka.utils.TestInfoUtils
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.config.{ConfigException, SaslConfigs}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Disabled, TestInfo}

import java.util.{Base64, Collections, Properties}
import no.nav.security.mock.oauth2.{MockOAuth2Server, OAuth2Config}
import no.nav.security.mock.oauth2.token.{KeyProvider, OAuth2TokenProvider}
import org.apache.kafka.common.{KafkaException, TopicPartition}
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs
import org.apache.kafka.common.errors.SaslAuthenticationException
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.security.oauthbearer.{JwtRetriever, OAuthBearerLoginCallbackHandler, OAuthBearerLoginModule, OAuthBearerValidatorCallbackHandler}
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.test.TestUtils
import org.junit.jupiter.api.Assertions.{assertDoesNotThrow, assertThrows}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource

import java.io.File
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption
import java.security.{KeyPairGenerator, PrivateKey}
import java.security.interfaces.RSAPublicKey
import java.util

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
  var privateKey: PrivateKey = _

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    // Step 1: Generate the key pair dynamically.
    val keyGen = KeyPairGenerator.getInstance("RSA")
    keyGen.initialize(2048)
    val keyPair = keyGen.generateKeyPair()

    privateKey = keyPair.getPrivate

    // Step 2: Create the RSA JWK from key pair.
    val rsaJWK = new RSAKey.Builder(keyPair.getPublic.asInstanceOf[RSAPublicKey])
      .privateKey(privateKey)
      .keyID("foo")
      .build()

    // Step 3: Create the OAuth server using the keys just created
    val keyProvider = new KeyProvider(Collections.singletonList(rsaJWK))
    val tokenProvider = new OAuth2TokenProvider(keyProvider)
    val oauthConfig = new OAuth2Config(false, null, null, false, tokenProvider)
    mockOAuthServer = new MockOAuth2Server(oauthConfig)

    mockOAuthServer.start()
    val tokenEndpointUrl = mockOAuthServer.tokenEndpointUrl(issuerId).url().toString
    val jwksUrl = mockOAuthServer.jwksUrl(issuerId).url().toString
    System.setProperty(BrokerSecurityConfigs.ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG, s"$tokenEndpointUrl,$jwksUrl")

    val listenerNamePrefix = s"listener.name.${listenerName.value().toLowerCase}"

    serverConfig.setProperty(s"$listenerNamePrefix.oauthbearer.${SaslConfigs.SASL_JAAS_CONFIG}", s"${classOf[OAuthBearerLoginModule].getName} required ;")
    serverConfig.setProperty(s"$listenerNamePrefix.oauthbearer.${SaslConfigs.SASL_OAUTHBEARER_EXPECTED_AUDIENCE}", issuerId)
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
    if (mockOAuthServer != null)
      mockOAuthServer.shutdown()

    closeSasl()
    super.tearDown()

    System.clearProperty(BrokerSecurityConfigs.ALLOWED_SASL_OAUTHBEARER_FILES_CONFIG)
    System.clearProperty(BrokerSecurityConfigs.ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG)
  }

  def defaultOAuthConfigs(): Properties = {
    val tokenEndpointUrl = mockOAuthServer.tokenEndpointUrl(issuerId).url().toString

    val configs = new Properties()
    configs.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol.name)
    configs.put(SaslConfigs.SASL_JAAS_CONFIG, jaasClientLoginModule(kafkaClientSaslMechanism))
    configs.put(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS, classOf[OAuthBearerLoginCallbackHandler].getName)
    configs.put(SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, tokenEndpointUrl)
    configs
  }

  def defaultClientCredentialsConfigs(): Properties = {
    val configs = defaultOAuthConfigs()
    configs.put(SaslConfigs.SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_ID, "test-client")
    configs.put(SaslConfigs.SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_SECRET, "test-secret")
    configs
  }

  def defaultJwtBearerConfigs(): Properties = {
    val configs = defaultOAuthConfigs()
    configs.put(SaslConfigs.SASL_JAAS_CONFIG, jaasClientLoginModule(kafkaClientSaslMechanism))
    configs.put(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS, classOf[OAuthBearerLoginCallbackHandler].getName)
    configs.put(SaslConfigs.SASL_OAUTHBEARER_JWT_RETRIEVER_CLASS, "org.apache.kafka.common.security.oauthbearer.JwtBearerJwtRetriever")
    configs
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testBasicClientCredentials(groupProtocol: String): Unit = {
    val configs = defaultClientCredentialsConfigs()
    assertDoesNotThrow(() => createProducer(configOverrides = configs))
    assertDoesNotThrow(() => createConsumer(configOverrides = configs))
    assertDoesNotThrow(() => createAdminClient(configOverrides = configs))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testBasicJwtBearer(groupProtocol: String): Unit = {
    val jwt = mockOAuthServer.issueToken(issuerId, "jdoe", "someaudience", Collections.singletonMap("scope", "test"))
    val assertionFile = TestUtils.tempFile(jwt.serialize())
    System.setProperty(BrokerSecurityConfigs.ALLOWED_SASL_OAUTHBEARER_FILES_CONFIG, assertionFile.getAbsolutePath)

    val configs = defaultJwtBearerConfigs()
    configs.put(SaslConfigs.SASL_OAUTHBEARER_ASSERTION_FILE, assertionFile.getAbsolutePath)

    assertDoesNotThrow(() => createProducer(configOverrides = configs))
    assertDoesNotThrow(() => createConsumer(configOverrides = configs))
    assertDoesNotThrow(() => createAdminClient(configOverrides = configs))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testBasicJwtBearer2(groupProtocol: String): Unit = {
    val privateKeyFile = generatePrivateKeyFile()
    System.setProperty(BrokerSecurityConfigs.ALLOWED_SASL_OAUTHBEARER_FILES_CONFIG, privateKeyFile.getAbsolutePath)

    val configs = defaultJwtBearerConfigs()
    configs.put(SaslConfigs.SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_FILE, privateKeyFile.getPath)
    configs.put(SaslConfigs.SASL_OAUTHBEARER_ASSERTION_CLAIM_AUD, "default")
    configs.put(SaslConfigs.SASL_OAUTHBEARER_ASSERTION_CLAIM_SUB, "kafka-client-test-sub")
    configs.put(SaslConfigs.SASL_OAUTHBEARER_SCOPE, "default")
    //    configs.put(SaslConfigs.SASL_OAUTHBEARER_SUB_CLAIM_NAME, "aud")

    assertDoesNotThrow(() => createProducer(configOverrides = configs))
    assertDoesNotThrow(() => createConsumer(configOverrides = configs))
    assertDoesNotThrow(() => createAdminClient(configOverrides = configs))
  }

  @Disabled("KAFKA-19394: Failure in ConsumerNetworkThread.initializeResources() can cause hangs on AsyncKafkaConsumer.close()")
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testJwtBearerWithMalformedAssertionFile(groupProtocol: String): Unit = {
    // Create the assertion file, but fill it with non-JWT garbage.
    val assertionFile = TestUtils.tempFile("CQEN*)Q#F)&)^#QNC")
    System.setProperty(BrokerSecurityConfigs.ALLOWED_SASL_OAUTHBEARER_FILES_CONFIG, assertionFile.getAbsolutePath)

    val configs = defaultJwtBearerConfigs()
    configs.put(SaslConfigs.SASL_OAUTHBEARER_ASSERTION_FILE, assertionFile.getAbsolutePath)

    assertThrows(classOf[KafkaException], () => createProducer(configOverrides = configs))
    assertThrows(classOf[KafkaException], () => createConsumer(configOverrides = configs))
    assertThrows(classOf[KafkaException], () => createAdminClient(configOverrides = configs))
  }

  @Disabled("KAFKA-19394: Failure in ConsumerNetworkThread.initializeResources() can cause hangs on AsyncKafkaConsumer.close()")
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testJwtBearerWithEmptyAssertionFile(groupProtocol: String): Unit = {
    // Create the assertion file, but leave it empty.
    val assertionFile = TestUtils.tempFile()
    System.setProperty(BrokerSecurityConfigs.ALLOWED_SASL_OAUTHBEARER_FILES_CONFIG, assertionFile.getAbsolutePath)

    val configs = defaultJwtBearerConfigs()
    configs.put(SaslConfigs.SASL_OAUTHBEARER_ASSERTION_FILE, assertionFile.getAbsolutePath)

    assertThrows(classOf[KafkaException], () => createProducer(configOverrides = configs))
    assertThrows(classOf[KafkaException], () => createConsumer(configOverrides = configs))
    assertThrows(classOf[KafkaException], () => createAdminClient(configOverrides = configs))
  }

  @Disabled("KAFKA-19394: Failure in ConsumerNetworkThread.initializeResources() can cause hangs on AsyncKafkaConsumer.close()")
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testJwtBearerWithMissingAssertionFile(groupProtocol: String): Unit = {
    val missingFileName = "/this/does/not/exist.txt"

    val configs = defaultJwtBearerConfigs()
    configs.put(SaslConfigs.SASL_OAUTHBEARER_ASSERTION_FILE, missingFileName)

    assertThrows(classOf[KafkaException], () => createProducer(configOverrides = configs))
    assertThrows(classOf[KafkaException], () => createConsumer(configOverrides = configs))
    assertThrows(classOf[KafkaException], () => createAdminClient(configOverrides = configs))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testUnsupportedJwtRetriever(groupProtocol: String): Unit = {
    val className = "org.apache.kafka.common.security.oauthbearer.ThisIsNotARealJwtRetriever"

    val configs = defaultOAuthConfigs()
    configs.put(SaslConfigs.SASL_OAUTHBEARER_JWT_RETRIEVER_CLASS, className)

    assertThrows(classOf[ConfigException], () => createProducer(configOverrides = configs))
    assertThrows(classOf[ConfigException], () => createConsumer(configOverrides = configs))
    assertThrows(classOf[ConfigException], () => createAdminClient(configOverrides = configs))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testAuthenticationErrorOnTamperedJwt(groupProtocol: String): Unit = {
    val className = classOf[TamperedJwtRetriever].getName

    val configs = defaultOAuthConfigs()
    configs.put(SaslConfigs.SASL_OAUTHBEARER_JWT_RETRIEVER_CLASS, className)

    val tp = new TopicPartition("test-topic", 0)

    val admin = createAdminClient(configOverrides = configs)
    TestUtils.assertFutureThrows(classOf[SaslAuthenticationException], admin.describeCluster().clusterId())

    val producer = createProducer(configOverrides = configs)
    assertThrows(classOf[SaslAuthenticationException], () => producer.partitionsFor(tp.topic()))

    val consumer = createConsumer(configOverrides = configs)
    consumer.assign(Collections.singleton(tp))
    assertThrows(classOf[SaslAuthenticationException], () => consumer.position(tp))
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

class TamperedJwtRetriever extends JwtRetriever {

  override def retrieve(): String = {
    "eyJhbGciOiAiSFMyNTYiLCAidHlwIjogIkpXVCJ9.eyJzdWIiOiAiMTIzNDU2Nzg5MCIsICJuYW1lIjogIkpvaG4gRG9lIiwgInJvbGUiOiAiYWRtaW4iLCAiaWF0IjogMTUxNjIzOTAyMiwgImV4cCI6IDE5MTYyMzkwMjJ9.vVT5ylQCGvb0B-wv1YXHjmlMd-DZKCThUt5-enry_sA"
  }
}
