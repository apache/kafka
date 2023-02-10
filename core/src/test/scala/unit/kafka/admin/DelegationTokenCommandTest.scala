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
package kafka.admin

import java.util

import kafka.admin.DelegationTokenCommand.DelegationTokenCommandOptions
import kafka.api.{KafkaSasl, SaslSetup}
import kafka.server.{BaseRequestTest, KafkaConfig}
import kafka.utils.{JaasTestUtils, TestUtils}
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig}
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test, TestInfo}

import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionException

class DelegationTokenCommandTest extends BaseRequestTest with SaslSetup {
  override protected def securityProtocol = SecurityProtocol.SASL_PLAINTEXT
  private val kafkaClientSaslMechanism = "PLAIN"
  private val kafkaServerSaslMechanisms = List("PLAIN")
  protected override val serverSaslProperties = Some(kafkaServerSaslProperties(kafkaServerSaslMechanisms, kafkaClientSaslMechanism))
  protected override val clientSaslProperties = Some(kafkaClientSaslProperties(kafkaClientSaslMechanism))
  var adminClient: Admin = _

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
    val renewer1 = "User:renewer1"
    val renewer2 = "User:renewer2"

    // create token1 with renewer1
    val tokenCreated = DelegationTokenCommand.createToken(adminClient, getCreateOpts(List(renewer1)))

    var tokens = DelegationTokenCommand.describeToken(adminClient, getDescribeOpts(List()))
    assertTrue(tokens.size == 1)
    val token1 = tokens.head
    assertEquals(token1, tokenCreated)

    // create token2 with renewer2
    val token2 = DelegationTokenCommand.createToken(adminClient, getCreateOpts(List(renewer2)))

    tokens = DelegationTokenCommand.describeToken(adminClient, getDescribeOpts(List()))
    assertTrue(tokens.size == 2)
    assertEquals(Set(token1, token2), tokens.toSet)

    //get tokens for renewer2
    tokens = DelegationTokenCommand.describeToken(adminClient, getDescribeOpts(List(renewer2)))
    assertTrue(tokens.size == 1)
    assertEquals(Set(token2), tokens.toSet)

    //test renewing tokens
    val expiryTimestamp = DelegationTokenCommand.renewToken(adminClient, getRenewOpts(token1.hmacAsBase64String()))
    val renewedToken = DelegationTokenCommand.describeToken(adminClient, getDescribeOpts(List(renewer1))).head
    assertEquals(expiryTimestamp, renewedToken.tokenInfo().expiryTimestamp())

    //test expire tokens
    DelegationTokenCommand.expireToken(adminClient, getExpireOpts(token1.hmacAsBase64String()))
    DelegationTokenCommand.expireToken(adminClient, getExpireOpts(token2.hmacAsBase64String()))

    tokens = DelegationTokenCommand.describeToken(adminClient, getDescribeOpts(List()))
    assertTrue(tokens.size == 0)

    //create token with invalid renewer principal type
    assertThrows(classOf[ExecutionException], () => DelegationTokenCommand.createToken(adminClient, getCreateOpts(List("Group:Renewer3"))))

    // try describing tokens for unknown owner
    assertTrue(DelegationTokenCommand.describeToken(adminClient, getDescribeOpts(List("User:Unknown"))).isEmpty)
  }

  private def getCreateOpts(renewers: List[String]): DelegationTokenCommandOptions = {
    val opts = ListBuffer("--bootstrap-server", bootstrapServers(), "--max-life-time-period", "-1",
      "--command-config", "testfile", "--create")
    renewers.foreach(renewer => opts ++= ListBuffer("--renewer-principal", renewer))
    new DelegationTokenCommandOptions(opts.toArray)
  }

  private def getDescribeOpts(owners: List[String]): DelegationTokenCommandOptions = {
    val opts = ListBuffer("--bootstrap-server", bootstrapServers(), "--command-config", "testfile", "--describe")
    owners.foreach(owner => opts ++= ListBuffer("--owner-principal", owner))
    new DelegationTokenCommandOptions(opts.toArray)
  }

  private def getRenewOpts(hmac: String): DelegationTokenCommandOptions = {
    val opts = Array("--bootstrap-server", bootstrapServers(), "--command-config", "testfile", "--renew",
      "--renew-time-period", "-1",
      "--hmac", hmac)
    new DelegationTokenCommandOptions(opts)
  }

  private def getExpireOpts(hmac: String): DelegationTokenCommandOptions = {
    val opts = Array("--bootstrap-server", bootstrapServers(), "--command-config", "testfile", "--expire",
      "--expiry-time-period", "-1",
      "--hmac", hmac)
    new DelegationTokenCommandOptions(opts)
  }

  @AfterEach
  override def tearDown(): Unit = {
    if (adminClient != null)
      adminClient.close()
    super.tearDown()
    closeSasl()
  }
}
