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
package kafka.server

import kafka.api.{IntegrationTestHarness, KafkaSasl, SaslSetup}
import kafka.security.JaasTestUtils
import kafka.utils.TestUtils
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, CreateDelegationTokenOptions, DescribeDelegationTokenOptions}
import org.apache.kafka.common.errors.{DelegationTokenNotFoundException, InvalidPrincipalTypeException}
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.SecurityUtils
import org.apache.kafka.server.config.DelegationTokenManagerConfigs
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, TestInfo}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.util
import scala.concurrent.ExecutionException
import scala.jdk.CollectionConverters._
import scala.jdk.javaapi.OptionConverters

class DelegationTokenRequestsTest extends IntegrationTestHarness with SaslSetup {
  override protected def securityProtocol = SecurityProtocol.SASL_PLAINTEXT
  private val kafkaClientSaslMechanism = "PLAIN"
  private val kafkaServerSaslMechanisms = List("PLAIN")
  protected override val serverSaslProperties = Some(kafkaServerSaslProperties(kafkaServerSaslMechanisms, kafkaClientSaslMechanism))
  protected override val clientSaslProperties = Some(kafkaClientSaslProperties(kafkaClientSaslMechanism))
  var adminClient: Admin = _

  override def brokerCount = 1

  this.serverConfig.setProperty(DelegationTokenManagerConfigs.DELEGATION_TOKEN_SECRET_KEY_CONFIG, "testKey")
  this.controllerConfig.setProperty(DelegationTokenManagerConfigs.DELEGATION_TOKEN_SECRET_KEY_CONFIG, "testKey")
  // Remove expired tokens every minute.
  this.serverConfig.setProperty(DelegationTokenManagerConfigs.DELEGATION_TOKEN_EXPIRY_CHECK_INTERVAL_MS_CONFIG, "5000")
  this.controllerConfig.setProperty(DelegationTokenManagerConfigs.DELEGATION_TOKEN_EXPIRY_CHECK_INTERVAL_MS_CONFIG, "5000")

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    startSasl(jaasSections(kafkaServerSaslMechanisms, Some(kafkaClientSaslMechanism), KafkaSasl, JaasTestUtils.KAFKA_SERVER_CONTEXT_NAME))
    super.setUp(testInfo)
  }

  private def createAdminConfig: util.Map[String, Object] = {
    val config = new util.HashMap[String, Object]
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers())
    val securityProps: util.Map[Object, Object] =
      JaasTestUtils.adminClientSecurityConfigs(securityProtocol, OptionConverters.toJava(trustStoreFile), OptionConverters.toJava(clientSaslProperties))
    securityProps.forEach { (key, value) => config.put(key.asInstanceOf[String], value) }
    config
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft", "zk"))
  def testDelegationTokenRequests(quorum: String): Unit = {
    adminClient = Admin.create(createAdminConfig)

    // create token1 with renewer1
    val renewer1 = List(SecurityUtils.parseKafkaPrincipal("User:renewer1")).asJava
    val createResult1 = adminClient.createDelegationToken(new CreateDelegationTokenOptions().renewers(renewer1))
    val tokenCreated = createResult1.delegationToken().get()

    TestUtils.waitUntilTrue(() => brokers.forall(server => server.tokenCache.tokens().size() == 1),
          "Timed out waiting for token to propagate to all servers")

    //test describe token
    var tokens = adminClient.describeDelegationToken().delegationTokens().get()
    assertEquals(1, tokens.size())
    var token1 = tokens.get(0)
    assertEquals(token1, tokenCreated)

    // create token2 with renewer2
    val renewer2 = List(SecurityUtils.parseKafkaPrincipal("User:renewer2")).asJava
    val createResult2 = adminClient.createDelegationToken(new CreateDelegationTokenOptions().renewers(renewer2))
    val token2 = createResult2.delegationToken().get()

    TestUtils.waitUntilTrue(() => brokers.forall(server => server.tokenCache.tokens().size() == 2),
          "Timed out waiting for token to propagate to all servers")

    //get all tokens
    tokens = adminClient.describeDelegationToken().delegationTokens().get()
    assertTrue(tokens.size() == 2)
    assertEquals(Set(token1, token2), tokens.asScala.toSet)

    //get tokens for renewer2
    tokens = adminClient.describeDelegationToken(new DescribeDelegationTokenOptions().owners(renewer2)).delegationTokens().get()
    assertTrue(tokens.size() == 1)
    assertEquals(Set(token2), tokens.asScala.toSet)

    //test renewing tokens
    val renewResult = adminClient.renewDelegationToken(token1.hmac())
    var expiryTimestamp = renewResult.expiryTimestamp().get()

    // Create a new delegtion token so we can wait for size of token cache to change
    // create token3 with renewer3
    val renewer3 = List(SecurityUtils.parseKafkaPrincipal("User:renewer3")).asJava
    val createResult3 = adminClient.createDelegationToken(new CreateDelegationTokenOptions().renewers(renewer3))
    val token3 = createResult3.delegationToken().get()

    TestUtils.waitUntilTrue(() => brokers.forall(server => server.tokenCache.tokens().size() == 3),
          "Timed out waiting for token to propagate to all servers")

    val describeResult = adminClient.describeDelegationToken()
    val tokenId = token1.tokenInfo().tokenId()

    token1 = describeResult.delegationTokens().get().asScala.filter(dt => dt.tokenInfo().tokenId() == tokenId).head
    assertEquals(expiryTimestamp, token1.tokenInfo().expiryTimestamp())

    //test expire tokens
    val expireResult1 = adminClient.expireDelegationToken(token1.hmac())
    expiryTimestamp = expireResult1.expiryTimestamp().get()

    val expireResult2 = adminClient.expireDelegationToken(token2.hmac())
    expiryTimestamp = expireResult2.expiryTimestamp().get()

    val expireResult3 = adminClient.expireDelegationToken(token3.hmac())
    expiryTimestamp = expireResult3.expiryTimestamp().get()

    TestUtils.waitUntilTrue(() => brokers.forall(server => server.tokenCache.tokens().size() == 0),
          "Timed out waiting for token to propagate to all servers")

    tokens = adminClient.describeDelegationToken().delegationTokens().get()
    assertTrue(tokens.size == 0)

    //create token with invalid principal type
    val renewer4 = List(SecurityUtils.parseKafkaPrincipal("Group:Renewer4")).asJava
    val createResult4 = adminClient.createDelegationToken(new CreateDelegationTokenOptions().renewers(renewer4))
    val createResult4Error = assertThrows(classOf[ExecutionException], () => createResult4.delegationToken().get())
    assertTrue(createResult4Error.getCause.isInstanceOf[InvalidPrincipalTypeException])

    // Try to renew a deleted token
    val renewResultPostDelete = adminClient.renewDelegationToken(token1.hmac())
    val renewResultPostDeleteError = assertThrows(classOf[ExecutionException], () => renewResultPostDelete.expiryTimestamp().get())
    assertTrue(renewResultPostDeleteError.getCause.isInstanceOf[DelegationTokenNotFoundException])

    // Create a DelegationToken with a short lifetime to validate the expire code
    val createResult5 = adminClient.createDelegationToken(new CreateDelegationTokenOptions()
      .renewers(renewer1)
      .maxlifeTimeMs(1 * 1000))
    val token5 = createResult5.delegationToken().get()

    TestUtils.waitUntilTrue(() => brokers.forall(server => server.tokenCache.tokens().size() == 1),
          "Timed out waiting for token to propagate to all servers")

    Thread.sleep(2 * 5 * 1000)

    tokens = adminClient.describeDelegationToken().delegationTokens().get()
    assertTrue(tokens.size == 0)

    // Try to expire a deleted token
    val expireResultPostDelete = adminClient.expireDelegationToken(token5.hmac())
    val expireResultPostDeleteError = assertThrows(classOf[ExecutionException], () => expireResultPostDelete.expiryTimestamp().get())
    assertTrue(expireResultPostDeleteError.getCause.isInstanceOf[DelegationTokenNotFoundException])

    // try describing tokens for unknown owner
    val unknownOwner = List(SecurityUtils.parseKafkaPrincipal("User:Unknown")).asJava
    tokens = adminClient.describeDelegationToken(new DescribeDelegationTokenOptions().owners(unknownOwner)).delegationTokens().get()
    assertTrue(tokens.isEmpty)
  }

  @AfterEach
  override def tearDown(): Unit = {
    if (adminClient != null)
      adminClient.close()
    super.tearDown()
    closeSasl()
  }
}
