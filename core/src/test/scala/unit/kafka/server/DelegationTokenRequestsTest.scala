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

import java.util

import kafka.admin.AdminClient
import kafka.api.{KafkaSasl, SaslSetup}
import kafka.utils.{JaasTestUtils, TestUtils}
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.SecurityUtils
import org.junit.Assert._
import org.junit.{After, Before, Test}

import scala.collection.JavaConverters._

class DelegationTokenRequestsTest extends BaseRequestTest with SaslSetup {
  override protected def securityProtocol = SecurityProtocol.SASL_PLAINTEXT
  private val kafkaClientSaslMechanism = "PLAIN"
  private val kafkaServerSaslMechanisms = List("PLAIN")
  protected override val serverSaslProperties = Some(kafkaServerSaslProperties(kafkaServerSaslMechanisms, kafkaClientSaslMechanism))
  protected override val clientSaslProperties = Some(kafkaClientSaslProperties(kafkaClientSaslMechanism))
  var adminClient: AdminClient = null

  override def numBrokers = 1

  @Before
  override def setUp(): Unit = {
    startSasl(jaasSections(kafkaServerSaslMechanisms, Some(kafkaClientSaslMechanism), KafkaSasl, JaasTestUtils.KafkaServerContextName))
    super.setUp()
  }

  def createAdminConfig():util.Map[String, Object] = {
    val config = new util.HashMap[String, Object]
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    val securityProps: util.Map[Object, Object] =
      TestUtils.adminClientSecurityConfigs(securityProtocol, trustStoreFile, clientSaslProperties)
    securityProps.asScala.foreach { case (key, value) => config.put(key.asInstanceOf[String], value) }
    config
  }

  override def generateConfigs = {
    val props = TestUtils.createBrokerConfigs(numBrokers, zkConnect,
      enableControlledShutdown = false, enableDeleteTopic = true,
      interBrokerSecurityProtocol = Some(securityProtocol),
      trustStoreFile = trustStoreFile, saslProperties = serverSaslProperties, enableToken = true)
    props.foreach(propertyOverrides)
    props.map(KafkaConfig.fromProps)
  }

  @Test
  def testDelegationTokenRequests(): Unit = {
    adminClient = AdminClient.create(createAdminConfig.asScala.toMap)

    // test creating token
    val renewer1 = List(SecurityUtils.parseKafkaPrincipal("User:" + JaasTestUtils.KafkaPlainUser))
    val tokenResult1 = adminClient.createToken(renewer1)
    assertEquals(Errors.NONE, tokenResult1._1)
    var token1 = adminClient.describeToken(null)._2.head
    assertEquals(token1, tokenResult1._2)

    //test renewing tokens
    val renewResponse = adminClient.renewToken(token1.hmacBuffer())
    assertEquals(Errors.NONE, renewResponse._1)

    token1 = adminClient.describeToken(null)._2.head
    assertEquals(renewResponse._2, token1.tokenInfo().expiryTimestamp())

    //test describe tokens
    val renewer2 = List(SecurityUtils.parseKafkaPrincipal("User:Renewer1"))
    val tokenResult2 = adminClient.createToken(renewer2)
    assertEquals(Errors.NONE, tokenResult2._1)
    val token2 = tokenResult2._2

    assertTrue(adminClient.describeToken(null)._2.size == 2)

    //test expire tokens tokens
    val expireResponse1 = adminClient.expireToken(token1.hmacBuffer())
    assertEquals(Errors.NONE, expireResponse1._1)

    val expireResponse2 = adminClient.expireToken(token2.hmacBuffer())
    assertEquals(Errors.NONE, expireResponse2._1)

    assertTrue(adminClient.describeToken(null)._2.size == 0)

    //create token with invalid principal type
    val renewer3 = List(SecurityUtils.parseKafkaPrincipal("Group:Renewer1"))
    val tokenResult3 = adminClient.createToken(renewer3)
    assertEquals(Errors.INVALID_PRINCIPAL_TYPE, tokenResult3._1)

  }

  @After
  override def tearDown(): Unit = {
    if (adminClient != null)
      adminClient.close()
    super.tearDown()
    closeSasl()
  }
}
