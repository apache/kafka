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

import java.nio.ByteBuffer
import java.util

import kafka.utils.TestUtils
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig}
import org.apache.kafka.common.errors.UnsupportedByAuthenticationException
import org.junit.{After, Before, Test}

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionException

class DelegationTokenRequestsOnPlainTextTest extends BaseRequestTest {
  var adminClient: AdminClient = null

  override def numBrokers = 1

  @Before
  override def setUp(): Unit = {
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
    intercept[ExecutionException](createResult.delegationToken().get()).getCause.isInstanceOf[UnsupportedByAuthenticationException]

    val describeResult = adminClient.describeDelegationToken()
    intercept[ExecutionException](describeResult.delegationTokens().get()).getCause.isInstanceOf[UnsupportedByAuthenticationException]

    val renewResult = adminClient.renewDelegationToken("".getBytes())
    intercept[ExecutionException](renewResult.expiryTimestamp().get()).getCause.isInstanceOf[UnsupportedByAuthenticationException]

    val expireResult = adminClient.expireDelegationToken("".getBytes())
    intercept[ExecutionException](expireResult.expiryTimestamp().get()).getCause.isInstanceOf[UnsupportedByAuthenticationException]
  }


  @After
  override def tearDown(): Unit = {
    if (adminClient != null)
      adminClient.close()
    super.tearDown()
  }
}
