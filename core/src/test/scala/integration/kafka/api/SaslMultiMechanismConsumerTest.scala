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

import java.io.File
import org.apache.kafka.common.protocol.SecurityProtocol
import kafka.server.KafkaConfig
import org.junit.Test
import kafka.utils.TestUtils
import scala.collection.JavaConverters._

class SaslMultiMechanismConsumerTest extends BaseConsumerTest with SaslTestHarness {
  override protected val zkSaslEnabled = true
  override protected val kafkaClientSaslMechanism = "PLAIN"
  override protected val kafkaServerSaslMechanisms = List("GSSAPI", "PLAIN")
  override protected def allKafkaClientSaslMechanisms = List("PLAIN", "GSSAPI")
  this.serverConfig.setProperty(KafkaConfig.ZkEnableSecureAclsProp, "true")
  override protected def securityProtocol = SecurityProtocol.SASL_SSL
  override protected lazy val trustStoreFile = Some(File.createTempFile("truststore", ".jks"))
  override protected val saslProperties = Some(kafkaSaslProperties(kafkaClientSaslMechanism, Some(kafkaServerSaslMechanisms)))

  @Test
  def testMultipleBrokerMechanisms() {

    val plainSaslProducer = producers(0)
    val plainSaslConsumer = consumers(0)

    val gssapiSaslProperties = kafkaSaslProperties("GSSAPI")
    val gssapiSaslProducer = TestUtils.createNewProducer(brokerList,
                                                         securityProtocol = this.securityProtocol,
                                                         trustStoreFile = this.trustStoreFile,
                                                         saslProperties = Some(gssapiSaslProperties))
    producers += gssapiSaslProducer
    val gssapiSaslConsumer = TestUtils.createNewConsumer(brokerList,
                                                         securityProtocol = this.securityProtocol,
                                                         trustStoreFile = this.trustStoreFile,
                                                         saslProperties = Some(gssapiSaslProperties))
    consumers += gssapiSaslConsumer
    val numRecords = 1000
    var startingOffset = 0

    // Test SASL/PLAIN producer and consumer
    sendRecords(plainSaslProducer, numRecords, tp)
    plainSaslConsumer.assign(List(tp).asJava)
    plainSaslConsumer.seek(tp, 0)
    consumeAndVerifyRecords(consumer = plainSaslConsumer, numRecords = numRecords, startingOffset = startingOffset)
    val plainCommitCallback = new CountConsumerCommitCallback()
    plainSaslConsumer.commitAsync(plainCommitCallback)
    awaitCommitCallback(plainSaslConsumer, plainCommitCallback)
    startingOffset += numRecords

    // Test SASL/GSSAPI producer and consumer
    sendRecords(gssapiSaslProducer, numRecords, tp)
    gssapiSaslConsumer.assign(List(tp).asJava)
    gssapiSaslConsumer.seek(tp, startingOffset)
    consumeAndVerifyRecords(consumer = gssapiSaslConsumer, numRecords = numRecords, startingOffset = startingOffset)
    val gssapiCommitCallback = new CountConsumerCommitCallback()
    gssapiSaslConsumer.commitAsync(gssapiCommitCallback)
    awaitCommitCallback(gssapiSaslConsumer, gssapiCommitCallback)
    startingOffset += numRecords

    // Test SASL/PLAIN producer and SASL/GSSAPI consumer
    sendRecords(plainSaslProducer, numRecords, tp)
    gssapiSaslConsumer.assign(List(tp).asJava)
    gssapiSaslConsumer.seek(tp, startingOffset)
    consumeAndVerifyRecords(consumer = gssapiSaslConsumer, numRecords = numRecords, startingOffset = startingOffset)
    startingOffset += numRecords

    // Test SASL/GSSAPI producer and SASL/PLAIN consumer
    sendRecords(gssapiSaslProducer, numRecords, tp)
    plainSaslConsumer.assign(List(tp).asJava)
    plainSaslConsumer.seek(tp, startingOffset)
    consumeAndVerifyRecords(consumer = plainSaslConsumer, numRecords = numRecords, startingOffset = startingOffset)

  }
}
