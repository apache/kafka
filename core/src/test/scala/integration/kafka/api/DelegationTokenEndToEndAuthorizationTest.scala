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

import kafka.security.JaasTestUtils

import java.util.Properties
import kafka.utils._
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, CreateDelegationTokenOptions, ScramCredentialInfo, UserScramCredentialAlteration, UserScramCredentialUpsertion, ScramMechanism => PublicScramMechanism}
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.apache.kafka.common.security.scram.internals.ScramMechanism
import org.apache.kafka.common.security.token.delegation.DelegationToken
import org.apache.kafka.metadata.storage.Formatter
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import org.junit.jupiter.api.{BeforeEach, TestInfo}

import scala.jdk.CollectionConverters._
import org.apache.kafka.server.config.DelegationTokenManagerConfigs

class DelegationTokenEndToEndAuthorizationTest extends EndToEndAuthorizationTest {

  val kafkaClientSaslMechanism = "SCRAM-SHA-256"
  val kafkaServerSaslMechanisms = ScramMechanism.mechanismNames.asScala.toList

  override protected def securityProtocol = SecurityProtocol.SASL_SSL

  override protected val serverSaslProperties = Some(kafkaServerSaslProperties(kafkaServerSaslMechanisms, kafkaClientSaslMechanism))
  override protected val clientSaslProperties = Some(kafkaClientSaslProperties(kafkaClientSaslMechanism))

  override val clientPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, JaasTestUtils.KAFKA_SCRAM_USER)
  private val clientPassword = JaasTestUtils.KAFKA_SCRAM_PASSWORD

  override val kafkaPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, JaasTestUtils.KAFKA_SCRAM_ADMIN)
  protected val kafkaPassword = JaasTestUtils.KAFKA_SCRAM_ADMIN_PASSWORD

  protected val privilegedAdminClientConfig = new Properties()

  this.serverConfig.setProperty(DelegationTokenManagerConfigs.DELEGATION_TOKEN_SECRET_KEY_CONFIG, "testKey")
  this.controllerConfig.setProperty(DelegationTokenManagerConfigs.DELEGATION_TOKEN_SECRET_KEY_CONFIG, "testKey")

  def createDelegationTokenOptions(): CreateDelegationTokenOptions = new CreateDelegationTokenOptions()

  override def configureSecurityBeforeServersStart(testInfo: TestInfo): Unit = {
    super.configureSecurityBeforeServersStart(testInfo)
  }

  // Create the admin credentials for KRaft as part of controller initialization

  override def addFormatterSettings(formatter: Formatter): Unit = {
    formatter.setClusterId("XcZZOzUqS4yHOjhMQB6JLQ")
    formatter.setScramArguments(
      List(s"SCRAM-SHA-256=[name=${JaasTestUtils.KAFKA_SCRAM_ADMIN},password=${JaasTestUtils.KAFKA_SCRAM_ADMIN_PASSWORD}]").asJava)
  }

  override def createPrivilegedAdminClient(): Admin = createScramAdminClient(kafkaClientSaslMechanism, kafkaPrincipal.getName, kafkaPassword)

  def createAdditionalCredentialsAfterServersStarted(): Unit = {}

  override def configureSecurityAfterServersStart(): Unit = {
    super.configureSecurityAfterServersStart()

    // create scram credential for user "scram-user"
    createScramCredentialsViaPrivilegedAdminClient(clientPrincipal.getName, clientPassword)
    waitForUserScramCredentialToAppearOnAllBrokers(clientPrincipal.getName, kafkaClientSaslMechanism)

    createAdditionalCredentialsAfterServersStarted()

    //create a token with "scram-user" credentials and a privileged token with scram-admin credentials
    val tokens = createDelegationTokens()
    val token = tokens._1
    val privilegedToken = tokens._2

    privilegedAdminClientConfig.putAll(adminClientConfig)

    // pass token to client jaas config
    val clientLoginContext = JaasTestUtils.tokenClientLoginModule(token.tokenInfo().tokenId(), token.hmacAsBase64String())
    producerConfig.put(SaslConfigs.SASL_JAAS_CONFIG, clientLoginContext)
    consumerConfig.put(SaslConfigs.SASL_JAAS_CONFIG, clientLoginContext)
    adminClientConfig.put(SaslConfigs.SASL_JAAS_CONFIG, clientLoginContext)
    val privilegedClientLoginContext = JaasTestUtils.tokenClientLoginModule(privilegedToken.tokenInfo().tokenId(), privilegedToken.hmacAsBase64String())
    privilegedAdminClientConfig.put(SaslConfigs.SASL_JAAS_CONFIG, privilegedClientLoginContext)
    superuserClientConfig.put(SaslConfigs.SASL_JAAS_CONFIG, privilegedClientLoginContext)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testCreateUserWithDelegationToken(quorum: String, groupProtocol: String): Unit = {
    val privilegedAdminClient = Admin.create(privilegedAdminClientConfig)
    try {
      val user = "user"
      val results = privilegedAdminClient.alterUserScramCredentials(List[UserScramCredentialAlteration](
        new UserScramCredentialUpsertion(user, new ScramCredentialInfo(PublicScramMechanism.SCRAM_SHA_256, 4096), "password")).asJava)
      assertEquals(1, results.values.size)
      val future = results.values.get(user)
      future.get // make sure we haven't completed exceptionally
    } finally {
      privilegedAdminClient.close()
    }
  }

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
      startSasl(jaasSections(kafkaServerSaslMechanisms, Option(kafkaClientSaslMechanism), KafkaSasl))
      super.setUp(testInfo)
      privilegedAdminClientConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers())
  }

  def assertTokenOwner(owner: KafkaPrincipal, token: DelegationToken): Unit = {
    assertEquals(owner, token.tokenInfo().owner())
  }

  def assertTokenRequester(requester: KafkaPrincipal, token: DelegationToken): Unit = {
    assertEquals(requester, token.tokenInfo().tokenRequester())
  }

  def assertToken(token: DelegationToken): Unit = {
    assertTokenOwner(clientPrincipal, token)
    assertTokenRequester(clientPrincipal, token)
  }

  def createTokenRequesterAdminClient(): Admin = {
    createScramAdminClient(kafkaClientSaslMechanism, clientPrincipal.getName, clientPassword)
  }

  def createDelegationTokens(): (DelegationToken, DelegationToken) = {
    createDelegationTokens(createDelegationTokenOptions)
  }

  def createDelegationTokens(createDelegationTokenOptions: () => CreateDelegationTokenOptions, assert: Boolean = true): (DelegationToken, DelegationToken) = {
    val adminClient = createTokenRequesterAdminClient()
    try {
      val privilegedAdminClient = createScramAdminClient(kafkaClientSaslMechanism, kafkaPrincipal.getName, kafkaPassword)
      try {
        val token = adminClient.createDelegationToken(createDelegationTokenOptions()).delegationToken().get()
        if (assert) {
          assertToken(token)
        }
        val privilegedToken = privilegedAdminClient.createDelegationToken().delegationToken().get()
        //wait for tokens to reach all the brokers
        TestUtils.waitUntilTrue(() => brokers.forall(server => server.tokenCache.tokens().size() == 2),
          "Timed out waiting for token to propagate to all servers")
        (token, privilegedToken)
      } finally {
        privilegedAdminClient.close()
      }
    } finally {
      adminClient.close()
    }
  }
}
