/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package integration.kafka.api

import java.util.Properties

import kafka.api.BaseProducerSendTest
import kafka.server.KafkaConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.header.internals.RecordHeaders
import org.junit.Test

class RecordHeaderProducerSendTest extends BaseProducerSendTest {
  @Test
  def testRecordHeaders(): Unit = {
    val producerProps = new Properties()
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    producerProps.put(ProducerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "true")
    val producer = new KafkaProducer[String, String](producerProps)
    try {
      var invalidRecordHeaders = new RecordHeaders()
      invalidRecordHeaders.add("RecordHeaderKey", "RecordHeaderValue".getBytes)
      // The record is invalid because it contain header with keys that doesn't start with a "_"
      // Keys that do start with a "_" are internal to the clients and are dropped at the producer.
      val invalidRecord = new ProducerRecord[String, String](
        topic,
        new Integer(0),
        "RecordKey",
        "RecordValue",
        invalidRecordHeaders
      )
      try {
        producer.send(invalidRecord).get
        throw new IllegalStateException("The invalid record should have thrown an exception")
      } catch {
        // Ignore the exception because a non internal header was introduced into the producer record
        case ignored: IllegalArgumentException =>
      }
  
      val validRecordHeaders = new RecordHeaders()
      validRecordHeaders.add("_RecordHeaderKey", "RecordHeaderValue".getBytes)
      val record = new ProducerRecord[String, String](
        topic,
        new Integer(0),
        "RecordKey",
        "RecordValue",
        validRecordHeaders
      )
      producer.send(record).get
    } finally {
      producer.close()
    }
  }


  override def overrideConfigs(): Properties = {
    val properties = new Properties()
    properties.put(KafkaConfig.InterBrokerProtocolVersionProp, "0.10.2")
    properties.put(KafkaConfig.LogMessageFormatVersionProp, "0.10.2")
    properties
  }
}
