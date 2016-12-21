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
import org.apache.kafka.common.protocol.SecurityProtocol
import org.apache.kafka.common.config.SaslConfigs
import kafka.utils.JaasTestUtils
import org.junit.Test
import java.util.Properties
import scala.collection.immutable.List
import scala.collection.JavaConverters._
import kafka.utils.TestUtils
import kafka.utils.JaasTestUtils.PlainLoginModule
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.errors.GroupAuthorizationException

class SaslPlainSslEndToEndAuthorizationTest extends EndToEndAuthorizationTest {
  override protected def securityProtocol = SecurityProtocol.SASL_SSL
  override protected def kafkaClientSaslMechanism = "PLAIN"
  override protected def kafkaServerSaslMechanisms = List("PLAIN")
  override val clientPrincipal = JaasTestUtils.KafkaPlainUser
  override val kafkaPrincipal = JaasTestUtils.KafkaPlainAdmin

  // Use JAAS configuration properties for clients so that dynamic JAAS configuration is also tested by this set of tests
  override protected def setJaasConfiguration(mode: SaslSetupMode, serverMechanisms: List[String], clientMechanisms: List[String],
      serverKeytabFile: Option[File] = None, clientKeytabFile: Option[File] = None) {
    super.setJaasConfiguration(mode, kafkaServerSaslMechanisms, List()) // create static config without client login contexts
    val clientLoginModule = JaasTestUtils.clientLoginModule(kafkaClientSaslMechanism, None)
    producerConfig.put(SaslConfigs.SASL_JAAS_CONFIG, clientLoginModule)
    consumerConfig.put(SaslConfigs.SASL_JAAS_CONFIG, clientLoginModule)
  }

  @Test
  def testConsumer1AllowedConsumer2NotAllowed {
    setAclsAndProduce()
    val consumer1 = consumers.head

    val consumer2Config = new Properties
    consumer2Config.putAll(consumerConfig)
    val clientLoginContext2 = PlainLoginModule(
          JaasTestUtils.KafkaPlainUser2,
          JaasTestUtils.KafkaPlainPassword2
        ).toJaasModule.toString.stripMargin
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
      fail("expected KafkaException as consumer2 has no access to topic")
    } catch {
      case e : GroupAuthorizationException => //expected
    }
  }

}
