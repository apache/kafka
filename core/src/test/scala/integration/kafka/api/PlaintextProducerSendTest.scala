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

package kafka.api

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.junit.Test

class PlaintextProducerSendTest extends BaseProducerSendTest {

  @Test
  def testSerializerConstructors() {
    try {
      createNewProducerWithNoSerializer(brokerList)
      fail("Instantiating a producer without specifying a serializer should cause a ConfigException")
    } catch {
      case ce : ConfigException => // this is ok
    }

    // create a producer with explicit serializers should succeed
    createNewProducerWithExplicitSerializer(brokerList)
  }

  private def createNewProducerWithNoSerializer(brokerList: String) : KafkaProducer[Array[Byte],Array[Byte]] = {
    val producerProps = new Properties()
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    return new KafkaProducer[Array[Byte],Array[Byte]](producerProps)
  }

  private def createNewProducerWithExplicitSerializer(brokerList: String) : KafkaProducer[Array[Byte],Array[Byte]] = {
    val producerProps = new Properties()
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    return new KafkaProducer[Array[Byte],Array[Byte]](producerProps, new ByteArraySerializer, new ByteArraySerializer)
  }

  @Test
  def testWrongSerializer() {
    // send a record with a wrong type should receive a serialization exception
    try {
      val producer = createProducerWithWrongSerializer(brokerList)
      val record5 = new ProducerRecord[Array[Byte], Array[Byte]](topic, new Integer(0), "key".getBytes, "value".getBytes)
      producer.send(record5)
      fail("Should have gotten a SerializationException")
    } catch {
      case se: SerializationException => // this is ok
    }
  }

  private def createProducerWithWrongSerializer(brokerList: String): KafkaProducer[Array[Byte], Array[Byte]] = {
    val producerProps = new Properties()
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    return new KafkaProducer[Array[Byte],Array[Byte]](producerProps)
  }

}
