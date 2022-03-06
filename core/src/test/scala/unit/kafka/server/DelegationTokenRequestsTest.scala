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

import kafka.api.{KafkaSasl, SaslSetup}
import kafka.utils.{JaasTestUtils, TestUtils}
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, CreateDelegationTokenOptions, DescribeDelegationTokenOptions}
import org.apache.kafka.common.errors.InvalidPrincipalTypeException
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.SecurityUtils
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test, TestInfo}

import java.util
import scala.concurrent.ExecutionException
import scala.jdk.CollectionConverters._

class DelegationTokenRequestsTest extends BaseRequestTest with SaslSetup {
  override protected def securityProtocol = SecurityProtocol.SASL_PLAINTEXT
  private val kafkaClientSaslMechanism = "PLAIN"
  private val kafkaServerSaslMechanisms = List("PLAIN")
  protected override val serverSaslProperties = Some(kafkaServerSaslProperties(kafkaServerSaslMechanisms, kafkaClientSaslMechanism))
  protected override val clientSaslProperties = Some(kafkaClientSaslProperties(kafkaClientSaslMechanism))
  var adminClient: Admin = null

  override def brokerCount = 1

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    startSasl(jaasSections(kafkaServerSaslMechanisms, Some(kafkaClientSaslMechanism), KafkaSasl, JaasTestUtils.KafkaServerContextName))
    super.setUp(testInfo)
  }

  override def generateConfigs = {
    val props = TestUtils.createBrokerConfigs(brokerCount, zkConnect,
      enableControlledShutdown = false,
      interBrokerSecurityProtocol = Some(securityProtocol),
      trustStoreFile = trustStoreFile, saslProperties = serverSaslProperties, enableToken = true)
    props.foreach(brokerPropertyOverrides)
    props.map(KafkaConfig.fromProps)
  }

  private def createAdminConfig: util.Map[String, Object] = {
    val config = new util.HashMap[String, Object]
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers())
    val securityProps: util.Map[Object, Object] =
      TestUtils.adminClientSecurityConfigs(securityProtocol, trustStoreFile, clientSaslProperties)
    securityProps.forEach { (key, value) => config.put(key.asInstanceOf[String], value) }
    config
  }

  @Test
  def testDelegationTokenRequests(): Unit = {
    adminClient = Admin.create(createAdminConfig)

    // create token1 with renewer1
    val renewer1 = List(SecurityUtils.parseKafkaPrincipal("User:renewer1")).asJava
    val createResult1 = adminClient.createDelegationToken(new CreateDelegationTokenOptions().renewers(renewer1))
    val tokenCreated = createResult1.delegationToken().get()

    //test describe token
    var tokens = adminClient.describeDelegationToken().delegationTokens().get()
    assertEquals(1, tokens.size())
    var token1 = tokens.get(0)
    assertEquals(token1, tokenCreated)

    // create token2 with renewer2
    val renewer2 = List(SecurityUtils.parseKafkaPrincipal("User:renewer2")).asJava
    val createResult2 = adminClient.createDelegationToken(new CreateDelegationTokenOptions().renewers(renewer2))
    val token2 = createResult2.delegationToken().get()

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

    val describeResult = adminClient.describeDelegationToken()
    val tokenId = token1.tokenInfo().tokenId()
    token1 = describeResult.delegationTokens().get().asScala.filter(dt => dt.tokenInfo().tokenId() == tokenId).head
    assertEquals(expiryTimestamp, token1.tokenInfo().expiryTimestamp())

    //test expire tokens
    val expireResult1 = adminClient.expireDelegationToken(token1.hmac())
    expiryTimestamp = expireResult1.expiryTimestamp().get()

    val expireResult2 = adminClient.expireDelegationToken(token2.hmac())
    expiryTimestamp = expireResult2.expiryTimestamp().get()

    tokens = adminClient.describeDelegationToken().delegationTokens().get()
    assertTrue(tokens.size == 0)

    //create token with invalid principal type
    val renewer3 = List(SecurityUtils.parseKafkaPrincipal("Group:Renewer3")).asJava
    val createResult3 = adminClient.createDelegationToken(new CreateDelegationTokenOptions().renewers(renewer3))
    assertThrows(classOf[ExecutionException], () => createResult3.delegationToken().get()).getCause.isInstanceOf[InvalidPrincipalTypeException]

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
