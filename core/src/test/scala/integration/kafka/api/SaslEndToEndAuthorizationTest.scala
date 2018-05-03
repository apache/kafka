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

import java.util.Properties

import kafka.utils.TestUtils
import kafka.utils.Implicits._
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.errors.{GroupAuthorizationException, TopicAuthorizationException}
import org.junit.{Before, Test}
import org.junit.Assert.{assertEquals, assertTrue}

import scala.collection.immutable.List
import scala.collection.JavaConverters._

abstract class SaslEndToEndAuthorizationTest extends EndToEndAuthorizationTest {
  override protected def securityProtocol = SecurityProtocol.SASL_SSL
  override protected val serverSaslProperties = Some(kafkaServerSaslProperties(kafkaServerSaslMechanisms, kafkaClientSaslMechanism))
  override protected val clientSaslProperties = Some(kafkaClientSaslProperties(kafkaClientSaslMechanism))
  
  protected def kafkaClientSaslMechanism: String
  protected def kafkaServerSaslMechanisms: List[String]
  
  @Before
  override def setUp(): Unit = {
    // create static config including client login context with credentials for JaasTestUtils 'client2'
    startSasl(jaasSections(kafkaServerSaslMechanisms, Option(kafkaClientSaslMechanism), Both))
    // set dynamic properties with credentials for JaasTestUtils 'client1' so that dynamic JAAS configuration is also
    // tested by this set of tests
    val clientLoginContext = jaasClientLoginModule(kafkaClientSaslMechanism)
    producerConfig.put(SaslConfigs.SASL_JAAS_CONFIG, clientLoginContext)
    consumerConfig.put(SaslConfigs.SASL_JAAS_CONFIG, clientLoginContext)
    super.setUp()
  }

  /**
    * Test with two consumers, each with different valid SASL credentials.
    * The first consumer succeeds because it is allowed by the ACL, 
    * the second one connects ok, but fails to consume messages due to the ACL.
    */
  @Test(timeout = 15000)
  def testTwoConsumersWithDifferentSaslCredentials(): Unit = {
    setAclsAndProduce()
    val consumer1 = consumers.head

    val consumer2Config = new Properties
    consumer2Config ++= consumerConfig
    // consumer2 retrieves its credentials from the static JAAS configuration, so we test also this path
    consumer2Config.remove(SaslConfigs.SASL_JAAS_CONFIG)
    consumer2Config.remove(SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS)

    val consumer2 = TestUtils.createNewConsumer(brokerList,
                                                securityProtocol = securityProtocol,
                                                trustStoreFile = trustStoreFile,
                                                saslProperties = clientSaslProperties,
                                                props = Some(consumer2Config))
    consumers += consumer2

    consumer1.assign(List(tp).asJava)
    consumer2.assign(List(tp).asJava)

    consumeRecords(consumer1, numRecords)

    try {
      consumeRecords(consumer2)
      fail("Expected exception as consumer2 has no access to topic or group")
    } catch {
      // Either exception is possible depending on the order that the first Metadata
      // and FindCoordinator requests are received
      case e: TopicAuthorizationException => assertTrue(e.unauthorizedTopics.contains(topic))
      case e: GroupAuthorizationException => assertEquals(group, e.groupId)
    }
  }
}
