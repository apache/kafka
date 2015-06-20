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
import java.util.concurrent.TimeUnit
import java.io.File

import kafka.consumer.SimpleConsumer
import kafka.integration.KafkaServerTestHarness
import kafka.message.Message
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.junit.Assert._
import org.junit.Test
import org.scalatest.junit.JUnit3Suite


class SSLProducerSendTest extends JUnit3Suite with KafkaServerTestHarness {
  val numServers = 1
  val trustStoreFile = File.createTempFile("truststore", ".jks")
  val overridingProps = new Properties()
  overridingProps.put(KafkaConfig.NumPartitionsProp, 4.toString)
  def generateConfigs() =
    TestUtils.createBrokerConfigs(numServers, zkConnect, false, enableSSL=true, trustStoreFile=Some(trustStoreFile)).map(KafkaConfig.fromProps(_, overridingProps))

  private var consumer1: SimpleConsumer = null
  private var consumer2: SimpleConsumer = null

  private val topic = "topic"
  private val numRecords = 100

  override def setUp() {
    super.setUp()

    // TODO: we need to migrate to new consumers when 0.9 is final
    consumer1 = new SimpleConsumer("localhost", servers(0).boundPort(), 100, 1024*1024, "")
    consumer2 = new SimpleConsumer("localhost", servers(0).boundPort(), 100, 1024*1024, "")

  }

  override def tearDown() {
    consumer1.close()
    consumer2.close()
    super.tearDown()
  }

  /**
    * testSendOffset checks the basic send API behavior
    *
    * 1. Send with null key/value/partition-id should be accepted; send with null topic should be rejected.
    * 2. Last message of the non-blocking send should return the correct offset metadata
    */
  @Test
  def testSendOffset() {
    var producer = TestUtils.createNewProducer(TestUtils.getSSLBrokerListStrFromServers(servers), enableSSL=true, trustStoreFile=Some(trustStoreFile))
    val partition = new Integer(0)

    object callback extends Callback {
      var offset = 0L
      def onCompletion(metadata: RecordMetadata, exception: Exception) {
        if (exception == null) {
          assertEquals(offset, metadata.offset())
          assertEquals(topic, metadata.topic())
          assertEquals(partition, metadata.partition())
          offset += 1
        } else {
          fail("Send callback returns the following exception", exception)
        }
      }
    }

    try {
      // create topic
      TestUtils.createTopic(zkClient, topic, 1, 1, servers)

      // send a normal record
      val record0 = new ProducerRecord[Array[Byte],Array[Byte]](topic, partition, "key".getBytes, "value".getBytes)
      assertEquals("Should have offset 0", 0L, producer.send(record0, callback).get.offset)


    } finally {
      if (producer != null) {
        producer.close()
        producer = null
      }
    }
  }
}
