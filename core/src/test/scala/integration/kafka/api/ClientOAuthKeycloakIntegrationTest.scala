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

import dasniko.testcontainers.keycloak.KeycloakContainer
import kafka.utils.{TestInfoUtils, TestUtils => KafkaTestUtils}
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs
import org.apache.kafka.test.TestUtils
import org.jose4j.jwk.RsaJsonWebKey
import org.jose4j.jws.{AlgorithmIdentifiers, JsonWebSignature}
import org.jose4j.jwt.JwtClaims
import org.junit.jupiter.api.Assertions.{assertDoesNotThrow, assertEquals, assertNotNull}
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.{AfterAll, BeforeAll}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import org.keycloak.admin.client.Keycloak
import org.keycloak.representations.idm.{ClientRepresentation, ProtocolMapperRepresentation, RealmRepresentation}
import org.testcontainers.DockerClientFactory

import java.security.{KeyPair, KeyPairGenerator, PrivateKey}
import java.security.interfaces.RSAPublicKey
import java.util.{Collections, Properties}
import scala.jdk.CollectionConverters._

/**
 * End-to-end tests of the OAuth client_credentials and client assertion flows against a real Keycloak
 * authorization server, run in a testcontainer. The tests are skipped when Docker is not available.
 */
class ClientOAuthKeycloakIntegrationTest extends AbstractClientOAuthIntegrationTest {

  import ClientOAuthKeycloakIntegrationTest._

  override protected def issuerUrl: String = realmIssuerUrl
  override protected def tokenEndpointUrl: String = realmTokenEndpointUrl
  override protected def jwksUrl: String = realmJwksUrl
  override protected def brokerAudience: String = BrokerAudience
  override protected def privateKey: PrivateKey = keyPair.getPrivate
  override protected def clientCredentialsClientId: String = SecretClientId
  override protected def clientCredentialsClientSecret: String = ClientSecret

  def defaultClientAssertionConfigs(): Properties = {
    val configs = defaultOAuthConfigs()
    configs.put(SaslConfigs.SASL_OAUTHBEARER_ASSERTION_CLAIM_ISS, ClientId)
    configs.put(SaslConfigs.SASL_OAUTHBEARER_ASSERTION_CLAIM_AUD, tokenEndpointUrl)
    configs.put(SaslConfigs.SASL_OAUTHBEARER_ASSERTION_CLAIM_SUB, ClientId)
    // Keycloak rejects client assertions without a jti, and Kafka does not add one by default.
    configs.put(SaslConfigs.SASL_OAUTHBEARER_ASSERTION_CLAIM_JTI_INCLUDE, "true")
    configs
  }

  // Keycloak rejects a re-used client assertion jti, so every client needs its own pre-signed assertion file.
  def newAssertionFileConfigs(): Properties = {
    val assertionFile = TestUtils.tempFile(signAssertion())
    val allowedFiles = System.getProperty(BrokerSecurityConfigs.ALLOWED_SASL_OAUTHBEARER_FILES_CONFIG, "")
    System.setProperty(BrokerSecurityConfigs.ALLOWED_SASL_OAUTHBEARER_FILES_CONFIG,
      if (allowedFiles.isEmpty) assertionFile.getAbsolutePath else s"$allowedFiles,${assertionFile.getAbsolutePath}")

    val configs = defaultOAuthConfigs()
    configs.put(SaslConfigs.SASL_OAUTHBEARER_ASSERTION_FILE, assertionFile.getAbsolutePath)
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
  def testClientAssertionProduceConsume(groupProtocol: String): Unit = {
    val topic = "client-assertion-test"
    val privateKeyFile = generatePrivateKeyFile()
    System.setProperty(BrokerSecurityConfigs.ALLOWED_SASL_OAUTHBEARER_FILES_CONFIG, privateKeyFile.getAbsolutePath)

    val configs = defaultClientAssertionConfigs()
    configs.put(SaslConfigs.SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_FILE, privateKeyFile.getPath)

    val admin = createAdminClient(configOverrides = configs)
    admin.createTopics(Collections.singletonList(new NewTopic(topic, 1, 1.toShort))).all().get()

    val producer = createProducer(configOverrides = configs)
    val record = new ProducerRecord[Array[Byte], Array[Byte]](topic, "key".getBytes, "value".getBytes)
    producer.send(record).get()

    val consumer = createConsumer(configOverrides = configs)
    consumer.subscribe(Collections.singletonList(topic))
    val records = KafkaTestUtils.consumeRecords(consumer, 1)
    assertEquals(1, records.size)
    assertEquals("value", new String(records.head.value()))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testClientAssertionFileBasedProduceConsume(groupProtocol: String): Unit = {
    val topic = "file-assertion-test"

    val admin = createAdminClient(configOverrides = newAssertionFileConfigs())
    admin.createTopics(Collections.singletonList(new NewTopic(topic, 1, 1.toShort))).all().get()

    val producer = createProducer(configOverrides = newAssertionFileConfigs())
    val record = new ProducerRecord[Array[Byte], Array[Byte]](topic, "key".getBytes, "value".getBytes)
    producer.send(record).get()

    val consumer = createConsumer(configOverrides = newAssertionFileConfigs())
    consumer.subscribe(Collections.singletonList(topic))
    val records = KafkaTestUtils.consumeRecords(consumer, 1)
    assertEquals(1, records.size)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testClientAssertionAdminOperations(groupProtocol: String): Unit = {
    val privateKeyFile = generatePrivateKeyFile()
    System.setProperty(BrokerSecurityConfigs.ALLOWED_SASL_OAUTHBEARER_FILES_CONFIG, privateKeyFile.getAbsolutePath)

    val configs = defaultClientAssertionConfigs()
    configs.put(SaslConfigs.SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_FILE, privateKeyFile.getPath)

    val admin = createAdminClient(configOverrides = configs)

    val clusterId = admin.describeCluster().clusterId().get()
    assertNotNull(clusterId)

    val topic = "admin-assertion-test"
    admin.createTopics(Collections.singletonList(new NewTopic(topic, 1, 1.toShort))).all().get()

    KafkaTestUtils.waitForAllPartitionsMetadata(brokers, topic, 1)

    val description = admin.describeTopics(Collections.singletonList(topic)).allTopicNames().get()
    assertNotNull(description.get(topic))
  }
}

object ClientOAuthKeycloakIntegrationTest {

  private val RealmName = "kafka-authz"
  private val SecretClientId = "kafka-secret-client"
  private val ClientId = "kafka-producer"
  private val ClientSecret = "test-client-secret-value"
  private val BrokerAudience = "kafka-broker"
  private val KeyId = "rsa-key-1"

  private var keycloak: KeycloakContainer = _
  private var keyPair: KeyPair = _

  private var realmIssuerUrl: String = _
  private var realmTokenEndpointUrl: String = _
  private var realmJwksUrl: String = _

  // Named differently from QuorumTestHarness.setUpClass/tearDownClass so that those static methods still run.
  @BeforeAll
  def startKeycloak(): Unit = {
    assumeTrue(DockerClientFactory.instance().isDockerAvailable, "Docker is not available - skipping integration tests")

    keycloak = new KeycloakContainer()
    keycloak.start()

    val authServerUrl = keycloak.getAuthServerUrl
    realmIssuerUrl = s"$authServerUrl/realms/$RealmName"
    realmTokenEndpointUrl = s"$realmIssuerUrl/protocol/openid-connect/token"
    realmJwksUrl = s"$realmIssuerUrl/protocol/openid-connect/certs"

    val keyGen = KeyPairGenerator.getInstance("RSA")
    keyGen.initialize(2048)
    keyPair = keyGen.generateKeyPair()

    val adminClient = keycloak.getKeycloakAdminClient
    try {
      createRealm(adminClient)
      createSecretClient(adminClient)
      createAssertionClient(adminClient)
    } finally {
      adminClient.close()
    }
  }

  @AfterAll
  def stopKeycloak(): Unit = {
    if (keycloak != null) {
      keycloak.stop()
      keycloak = null
    }
  }

  private def createRealm(adminClient: Keycloak): Unit = {
    val realm = new RealmRepresentation()
    realm.setRealm(RealmName)
    realm.setEnabled(true)
    realm.setSslRequired("none")
    realm.setAccessTokenLifespan(300)
    adminClient.realms().create(realm)
  }

  private def createSecretClient(adminClient: Keycloak): Unit = {
    val client = newServiceAccountClient(SecretClientId, "Kafka Client (Secret Auth)", "client-secret")
    client.setSecret(ClientSecret)
    createClient(adminClient, client)
  }

  private def createAssertionClient(adminClient: Keycloak): Unit = {
    val jwk = new RsaJsonWebKey(keyPair.getPublic.asInstanceOf[RSAPublicKey])
    jwk.setKeyId(KeyId)
    jwk.setUse("sig")
    jwk.setAlgorithm(AlgorithmIdentifiers.RSA_USING_SHA256)

    val client = newServiceAccountClient(ClientId, "Kafka Producer (Assertion Auth)", "client-jwt")
    client.setAttributes(Map(
      "use.jwks.url" -> "false",
      // Enable inline JWKS string for public key verification (Keycloak 26.0+)
      "use.jwks.string" -> "true",
      // Register the public key as inline JWKS (attribute name: jwks.string)
      "jwks.string" -> s"""{"keys":[${jwk.toJson}]}"""
    ).asJava)
    createClient(adminClient, client)
  }

  private def newServiceAccountClient(clientId: String, name: String, authenticatorType: String): ClientRepresentation = {
    // Without this mapper the access token's aud is not the one the broker is configured to expect.
    val audienceMapper = new ProtocolMapperRepresentation()
    audienceMapper.setName("kafka-broker-audience")
    audienceMapper.setProtocol("openid-connect")
    audienceMapper.setProtocolMapper("oidc-audience-mapper")
    audienceMapper.setConfig(Map(
      "included.custom.audience" -> BrokerAudience,
      "access.token.claim" -> "true",
      "id.token.claim" -> "false"
    ).asJava)

    val client = new ClientRepresentation()
    client.setClientId(clientId)
    client.setName(name)
    client.setEnabled(true)
    client.setClientAuthenticatorType(authenticatorType)
    client.setPublicClient(false)
    client.setServiceAccountsEnabled(true)
    client.setStandardFlowEnabled(false)
    client.setDirectAccessGrantsEnabled(false)
    client.setProtocol("openid-connect")
    client.setProtocolMappers(Collections.singletonList(audienceMapper))
    client
  }

  private def createClient(adminClient: Keycloak, client: ClientRepresentation): Unit = {
    val response = adminClient.realm(RealmName).clients().create(client)
    try {
      assertEquals(201, response.getStatus, s"Failed to create Keycloak client ${client.getClientId}")
    } finally {
      response.close()
    }
  }

  private def signAssertion(): String = {
    val claims = new JwtClaims()
    claims.setIssuer(ClientId)
    claims.setSubject(ClientId)
    claims.setAudience(realmTokenEndpointUrl)
    claims.setIssuedAtToNow()
    claims.setExpirationTimeMinutesInTheFuture(5f)
    claims.setGeneratedJwtId()

    val jws = new JsonWebSignature()
    jws.setPayload(claims.toJson)
    jws.setKey(keyPair.getPrivate)
    jws.setKeyIdHeaderValue(KeyId)
    jws.setAlgorithmHeaderValue(AlgorithmIdentifiers.RSA_USING_SHA256)
    jws.getCompactSerialization
  }
}
