/**
  * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
  * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
  * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
  * License. You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
  * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
  * specific language governing permissions and limitations under the License.
  */
package kafka.api

import kafka.security.JaasTestUtils
import kafka.utils.TestUtils
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.server.config.ZkConfigs
import org.junit.jupiter.api.{AfterEach, BeforeEach, TestInfo, Timeout}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import scala.jdk.CollectionConverters._

@Timeout(600)
class SaslMultiMechanismConsumerTest extends BaseConsumerTest with SaslSetup {
  private val kafkaClientSaslMechanism = "PLAIN"
  private val kafkaServerSaslMechanisms = List("GSSAPI", "PLAIN")
  this.serverConfig.setProperty(ZkConfigs.ZK_ENABLE_SECURE_ACLS_CONFIG, "true")
  override protected def securityProtocol = SecurityProtocol.SASL_SSL
  override protected lazy val trustStoreFile = Some(TestUtils.tempFile("truststore", ".jks"))
  override protected val serverSaslProperties = Some(kafkaServerSaslProperties(kafkaServerSaslMechanisms, kafkaClientSaslMechanism))
  override protected val clientSaslProperties = Some(kafkaClientSaslProperties(kafkaClientSaslMechanism))

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    startSasl(jaasSections(kafkaServerSaslMechanisms, Some(kafkaClientSaslMechanism), Both,
      JaasTestUtils.KAFKA_SERVER_CONTEXT_NAME))
    super.setUp(testInfo)
  }

  @AfterEach
  override def tearDown(): Unit = {
    super.tearDown()
    closeSasl()
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testMultipleBrokerMechanisms(quorum: String): Unit = {
    val plainSaslProducer = createProducer()
    val plainSaslConsumer = createConsumer()

    val gssapiSaslProperties = kafkaClientSaslProperties("GSSAPI", dynamicJaasConfig = true)
    val gssapiSaslProducer = createProducer(configOverrides = gssapiSaslProperties)
    val gssapiSaslConsumer = createConsumer(configOverrides = gssapiSaslProperties)
    val numRecords = 1000
    var startingOffset = 0

    // Test SASL/PLAIN producer and consumer
    var startingTimestamp = System.currentTimeMillis()
    sendRecords(plainSaslProducer, numRecords, tp, startingTimestamp = startingTimestamp)
    plainSaslConsumer.assign(List(tp).asJava)
    plainSaslConsumer.seek(tp, 0)
    consumeAndVerifyRecords(consumer = plainSaslConsumer, numRecords = numRecords, startingOffset = startingOffset,
      startingTimestamp = startingTimestamp)
    sendAndAwaitAsyncCommit(plainSaslConsumer)
    startingOffset += numRecords

    // Test SASL/GSSAPI producer and consumer
    startingTimestamp = System.currentTimeMillis()
    sendRecords(gssapiSaslProducer, numRecords, tp, startingTimestamp = startingTimestamp)
    gssapiSaslConsumer.assign(List(tp).asJava)
    gssapiSaslConsumer.seek(tp, startingOffset)
    consumeAndVerifyRecords(consumer = gssapiSaslConsumer, numRecords = numRecords, startingOffset = startingOffset,
      startingTimestamp = startingTimestamp)
    sendAndAwaitAsyncCommit(gssapiSaslConsumer)
    startingOffset += numRecords

    // Test SASL/PLAIN producer and SASL/GSSAPI consumer
    startingTimestamp = System.currentTimeMillis()
    sendRecords(plainSaslProducer, numRecords, tp, startingTimestamp = startingTimestamp)
    gssapiSaslConsumer.assign(List(tp).asJava)
    gssapiSaslConsumer.seek(tp, startingOffset)
    consumeAndVerifyRecords(consumer = gssapiSaslConsumer, numRecords = numRecords, startingOffset = startingOffset,
      startingTimestamp = startingTimestamp)
    startingOffset += numRecords

    // Test SASL/GSSAPI producer and SASL/PLAIN consumer
    startingTimestamp = System.currentTimeMillis()
    sendRecords(gssapiSaslProducer, numRecords, tp, startingTimestamp = startingTimestamp)
    plainSaslConsumer.assign(List(tp).asJava)
    plainSaslConsumer.seek(tp, startingOffset)
    consumeAndVerifyRecords(consumer = plainSaslConsumer, numRecords = numRecords, startingOffset = startingOffset,
      startingTimestamp = startingTimestamp)
  }

}
