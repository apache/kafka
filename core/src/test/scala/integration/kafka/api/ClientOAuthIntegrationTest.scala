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
import kafka.utils.TestInfoUtils
import org.apache.kafka.common.config.{ConfigException, SaslConfigs}
import org.junit.jupiter.api.Disabled

import java.util.{Collections, Properties}
import no.nav.security.mock.oauth2.{MockOAuth2Server, OAuth2Config}
import no.nav.security.mock.oauth2.token.{KeyProvider, OAuth2TokenProvider}
import org.apache.kafka.common.{KafkaException, TopicPartition}
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs
import org.apache.kafka.common.errors.SaslAuthenticationException
import org.apache.kafka.common.security.oauthbearer.{JwtRetriever, OAuthBearerLoginCallbackHandler}
import org.apache.kafka.test.TestUtils
import org.junit.jupiter.api.Assertions.{assertDoesNotThrow, assertThrows}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource

import java.nio.file.Files
import java.security.{KeyPairGenerator, PrivateKey}
import java.security.interfaces.RSAPublicKey

/**
 * Integration tests for the jwt-bearer grant against a mock OAuth server.
 */
class ClientOAuthIntegrationTest extends AbstractClientOAuthIntegrationTest {

  val issuerId = "default"
  var mockOAuthServer: MockOAuth2Server = _
  var privateKey: PrivateKey = _

  override protected def issuerUrl: String = mockOAuthServer.issuerUrl(issuerId).toString
  override protected def tokenEndpointUrl: String = mockOAuthServer.tokenEndpointUrl(issuerId).url().toString
  override protected def jwksUrl: String = mockOAuthServer.jwksUrl(issuerId).url().toString
  override protected def brokerAudience: String = issuerId
  override protected def clientCredentialsClientId: String = "test-client"
  override protected def clientCredentialsClientSecret: String = "test-secret"

  override protected def startOAuthServer(): Unit = {
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
  }

  override protected def stopOAuthServer(): Unit = {
    if (mockOAuthServer != null)
      mockOAuthServer.shutdown()
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
  def testBasicJwtBearer(groupProtocol: String): Unit = {
    val jwt = mockOAuthServer.issueToken(issuerId, "jdoe", "someaudience", Collections.singletonMap("scope", "test"))
    val assertionFile = Files.writeString(tempDir.resolve("assertion.jwt"), jwt.serialize()).toFile
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
    val assertionFile = Files.writeString(tempDir.resolve("assertion.jwt"), "CQEN*)Q#F)&)^#QNC").toFile
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
    val assertionFile = Files.createFile(tempDir.resolve("assertion.jwt")).toFile
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
}

class TamperedJwtRetriever extends JwtRetriever {

  override def retrieve(): String = {
    "eyJhbGciOiAiSFMyNTYiLCAidHlwIjogIkpXVCJ9.eyJzdWIiOiAiMTIzNDU2Nzg5MCIsICJuYW1lIjogIkpvaG4gRG9lIiwgInJvbGUiOiAiYWRtaW4iLCAiaWF0IjogMTUxNjIzOTAyMiwgImV4cCI6IDE5MTYyMzkwMjJ9.vVT5ylQCGvb0B-wv1YXHjmlMd-DZKCThUt5-enry_sA"
  }
}
