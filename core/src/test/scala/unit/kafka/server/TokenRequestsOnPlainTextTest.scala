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

import kafka.admin.AdminClient
import kafka.utils.TestUtils
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.common.protocol.Errors
import org.junit.Assert._
import org.junit.{After, Before, Test}

import scala.collection.JavaConverters._

class TokenRequestsOnPlainTextTest extends BaseRequestTest {
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
    adminClient = AdminClient.create(createAdminConfig.asScala.toMap)

    val createResponse = adminClient.createToken(List())
    assertEquals(Errors.TOKEN_REQUEST_NOT_ALLOWED, createResponse._1)

    val describeResponse = adminClient.describeToken(List())
    assertEquals(Errors.TOKEN_REQUEST_NOT_ALLOWED, describeResponse._1)

    //test renewing tokens
    val renewResponse = adminClient.renewToken(ByteBuffer.wrap("".getBytes()))
    assertEquals(Errors.TOKEN_REQUEST_NOT_ALLOWED, renewResponse._1)

    //test expire tokens tokens
    val expireResponse = adminClient.expireToken(ByteBuffer.wrap("".getBytes()))
    assertEquals(Errors.TOKEN_REQUEST_NOT_ALLOWED, expireResponse._1)
  }


  @After
  override def tearDown(): Unit = {
    if (adminClient != null)
      adminClient.close()
    super.tearDown()
  }
}
