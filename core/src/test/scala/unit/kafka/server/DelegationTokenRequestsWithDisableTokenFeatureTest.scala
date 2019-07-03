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

import kafka.api.{KafkaSasl, SaslSetup}
import kafka.utils.{JaasTestUtils, TestUtils}
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig}
import org.apache.kafka.common.errors.DelegationTokenDisabledException
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.junit.{After, Before, Test}
import org.scalatest.Assertions.intercept

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionException

class DelegationTokenRequestsWithDisableTokenFeatureTest extends BaseRequestTest with SaslSetup {
  override protected def securityProtocol = SecurityProtocol.SASL_PLAINTEXT
  private val kafkaClientSaslMechanism = "PLAIN"
  private val kafkaServerSaslMechanisms = List("PLAIN")
  protected override val serverSaslProperties = Some(kafkaServerSaslProperties(kafkaServerSaslMechanisms, kafkaClientSaslMechanism))
  protected override val clientSaslProperties = Some(kafkaClientSaslProperties(kafkaClientSaslMechanism))
  var adminClient: AdminClient = null

  override def brokerCount = 1

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

  @Test
  def testDelegationTokenRequests(): Unit = {
    adminClient = AdminClient.create(createAdminConfig)

    val createResult = adminClient.createDelegationToken()
    intercept[ExecutionException](createResult.delegationToken().get()).getCause.isInstanceOf[DelegationTokenDisabledException]

    val describeResult = adminClient.describeDelegationToken()
    intercept[ExecutionException](describeResult.delegationTokens().get()).getCause.isInstanceOf[DelegationTokenDisabledException]

    val renewResult = adminClient.renewDelegationToken("".getBytes())
    intercept[ExecutionException](renewResult.expiryTimestamp().get()).getCause.isInstanceOf[DelegationTokenDisabledException]

    val expireResult = adminClient.expireDelegationToken("".getBytes())
    intercept[ExecutionException](expireResult.expiryTimestamp().get()).getCause.isInstanceOf[DelegationTokenDisabledException]
  }

  @After
  override def tearDown(): Unit = {
    if (adminClient != null)
      adminClient.close()
    super.tearDown()
    closeSasl()
  }
}
