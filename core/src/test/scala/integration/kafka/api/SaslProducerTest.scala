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

import java.util
import java.util.Properties

import kafka.admin.AdminUtils
import kafka.common.{ErrorMapping, FailedToSendMessageException}
import kafka.consumer.SimpleConsumer
import kafka.message.Message
import kafka.producer.KeyedMessage
import kafka.serializer.StringEncoder
import kafka.server.{KafkaConfig, KafkaRequestHandler, KafkaServer}
import kafka.utils._
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.common.protocol.SecurityProtocol
import org.apache.kafka.clients.producer._

import org.junit.Assert._
import org.junit.{After, Before, Test}


class SaslProducerTest extends SaslTestHarness {
  private val brokerId1 = 0
  private var server1: KafkaServer = null
  private var consumer1: SimpleConsumer = null
  private var servers = List.empty[KafkaServer]

  @Before
  override def setUp() {
    super.setUp()
    // set up 2 brokers with 4 partitions each
    val props1 = TestUtils.createBrokerConfig(brokerId1, zkConnect, false, enableSasl = true)
    props1.put("num.partitions", "1")
    props1.put(KafkaConfig.DefaultReplicationFactorProp, "1")
    val config1 = KafkaConfig.fromProps(props1)

    server1 = TestUtils.createServer(config1)
    servers = List(server1)
    consumer1 = new SimpleConsumer("localhost", server1.boundPort(SecurityProtocol.PLAINTEXT), 1000000, 64*1024, "")
  }

  @After
  override def tearDown() {
    consumer1.close()
    server1.shutdown
    CoreUtils.rm(server1.config.logDirs)
    super.tearDown()
  }

  @Test
  def testProduceAndConsume() {
    val topic = "new-topic"
    val producer = TestUtils.createNewProducer(TestUtils.getSaslBrokerListStrFromServers(servers), enableSasl=true)
    val partition = new Integer(0)
    val numRecords = 100

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

      // create topic with 1 partition and await leadership
      TestUtils.createTopic(zkClient, topic, 1, 1, servers)

      val record0 = new ProducerRecord[Array[Byte],Array[Byte]](topic, partition, "key".getBytes, "value".getBytes)
      assertEquals("Should have offset 0", 0L, producer.send(record0, callback).get.offset)

      // send a record with null value should be ok
      val record1 = new ProducerRecord[Array[Byte],Array[Byte]](topic, partition, "key".getBytes, null)
      assertEquals("Should have offset 1", 1L, producer.send(record1, callback).get.offset)

      // send a record with null key should be ok
      val record2 = new ProducerRecord[Array[Byte],Array[Byte]](topic, partition, null, "value".getBytes)
      assertEquals("Should have offset 2", 2L, producer.send(record2, callback).get.offset)

      // send a record with null part id should be ok
      val record3 = new ProducerRecord[Array[Byte],Array[Byte]](topic, null, "key".getBytes, "value".getBytes)
      assertEquals("Should have offset 3", 3L, producer.send(record3, callback).get.offset)

      // send a record with null topic should fail
      try {
        val record4 = new ProducerRecord[Array[Byte],Array[Byte]](null, partition, "key".getBytes, "value".getBytes)
        producer.send(record4, callback)
        fail("Should not allow sending a record without topic")
      } catch {
        case iae: IllegalArgumentException => // this is ok
        case e: Throwable => fail("Only expecting IllegalArgumentException", e)
      }

      // non-blocking send a list of records with producer
      for (i <- 1 to numRecords)
        producer.send(record0, callback)
      // check that all messages have been acked via offset
      assertEquals("Should have offset " + numRecords + 4L, numRecords + 4L, producer.send(record0, callback).get.offset)


    } finally {
      producer.close()
    }
  }
}
