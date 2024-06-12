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

import kafka.utils.TestUtils
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig}
import org.apache.kafka.common.errors.UnsupportedByAuthenticationException
import org.junit.jupiter.api.{AfterEach, BeforeEach, TestInfo}
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import scala.concurrent.ExecutionException

class DelegationTokenRequestsOnPlainTextTest extends BaseRequestTest {
  var adminClient: Admin = _

  override def brokerCount = 1

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    super.setUp(testInfo)
  }

  def createAdminConfig: util.Map[String, Object] = {
    val config = new util.HashMap[String, Object]
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers())
    val securityProps: util.Map[Object, Object] =
      TestUtils.adminClientSecurityConfigs(securityProtocol, trustStoreFile, clientSaslProperties)
    securityProps.forEach { (key, value) => config.put(key.asInstanceOf[String], value) }
    config
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft", "zk"))
  def testDelegationTokenRequests(quorum: String): Unit = {
    adminClient = Admin.create(createAdminConfig)

    val createResult = adminClient.createDelegationToken()
    assertThrows(classOf[ExecutionException], () => createResult.delegationToken().get()).getCause.isInstanceOf[UnsupportedByAuthenticationException]

    val describeResult = adminClient.describeDelegationToken()
    assertThrows(classOf[ExecutionException], () => describeResult.delegationTokens().get()).getCause.isInstanceOf[UnsupportedByAuthenticationException]

    val renewResult = adminClient.renewDelegationToken("".getBytes())
    assertThrows(classOf[ExecutionException], () => renewResult.expiryTimestamp().get()).getCause.isInstanceOf[UnsupportedByAuthenticationException]

    val expireResult = adminClient.expireDelegationToken("".getBytes())
    assertThrows(classOf[ExecutionException], () => expireResult.expiryTimestamp().get()).getCause.isInstanceOf[UnsupportedByAuthenticationException]
  }


  @AfterEach
  override def tearDown(): Unit = {
    if (adminClient != null)
      adminClient.close()
    super.tearDown()
  }
}
