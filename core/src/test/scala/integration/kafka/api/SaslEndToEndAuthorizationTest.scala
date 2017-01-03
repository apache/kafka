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

import java.io.File
import java.util.Properties

import kafka.utils.{JaasTestUtils,TestUtils}
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.errors.GroupAuthorizationException
import org.junit.{Before,Test}

import scala.collection.immutable.List
import scala.collection.JavaConverters._

abstract class SaslEndToEndAuthorizationTest extends EndToEndAuthorizationTest {

  override protected val saslProperties = Some(kafkaSaslProperties(kafkaClientSaslMechanism, Some(kafkaServerSaslMechanisms)))
  protected var clientKeytabFile: Option[File] = None
  
  protected def kafkaClientSaslMechanism : String
  protected def kafkaServerSaslMechanisms : List[String]
  protected def clientLoginContext2 : String

  
  @Before
  override def setUp {
    startSasl(Both, List(kafkaClientSaslMechanism), kafkaServerSaslMechanisms)
    super.setUp
  }

  // Use JAAS configuration properties for clients so that dynamic JAAS configuration is also tested by this set of tests
  override protected def setJaasConfiguration(mode: SaslSetupMode, serverMechanisms: List[String], clientMechanisms: List[String],
      serverKeytabFile: Option[File] = None, clientKeytabFile: Option[File] = None) {
    this.clientKeytabFile = clientKeytabFile
    super.setJaasConfiguration(mode, kafkaServerSaslMechanisms, List(), serverKeytabFile, clientKeytabFile)
    val clientLoginContext = JaasTestUtils.clientLoginModule(kafkaClientSaslMechanism, clientKeytabFile)
    producerConfig.put(SaslConfigs.SASL_JAAS_CONFIG, clientLoginContext)
    consumerConfig.put(SaslConfigs.SASL_JAAS_CONFIG, clientLoginContext)
  }

  /**
    * Tests with two consumers with different SASL credentials, the first one with proper ACLs succeeds, 
    * while a second fails to consume messages.
    */
  @Test
  def testConsumer1AllowedConsumer2NotAllowed {
    setAclsAndProduce()
    val consumer1 = consumers.head

    val consumer2Config = new Properties
    consumer2Config.putAll(consumerConfig)

    consumer2Config.setProperty(SaslConfigs.SASL_JAAS_CONFIG, clientLoginContext2)

    val consumer2 = TestUtils.createNewConsumer(brokerList,
                                  securityProtocol = this.securityProtocol,
                                  trustStoreFile = this.trustStoreFile,
                                  saslProperties = this.saslProperties,
                                  props = Some(consumer2Config))
    consumers += consumer2

    consumer1.assign(List(tp).asJava)
    consumer2.assign(List(tp).asJava)

    consumeRecords(consumer1, numRecords)

    try {
      consumeRecords(consumer2, timeout = 5000)
      fail("expected KafkaException as consumer2 has no access to group")
    } catch {
      case e : GroupAuthorizationException => //expected
    }
  }
}
