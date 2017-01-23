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

package kafka.api.test

import java.util.{Properties, Collection, ArrayList}

import org.junit.runners.Parameterized
import org.junit.runner.RunWith
import org.junit.runners.Parameterized.Parameters
import org.junit.{After, Before, Test}
import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer, ProducerConfig}
import org.junit.Assert._

import kafka.api.FetchRequestBuilder
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.consumer.SimpleConsumer
import kafka.message.Message
import kafka.zk.ZooKeeperTestHarness
import kafka.utils.{CoreUtils, TestUtils}


@RunWith(value = classOf[Parameterized])
class ProducerCompressionTest(compression: String) extends ZooKeeperTestHarness {
  private val brokerId = 0
  private var server: KafkaServer = null

  private val topic = "topic"
  private val numRecords = 2000

  @Before
  override def setUp() {
    super.setUp()

    val props = TestUtils.createBrokerConfig(brokerId, zkConnect)
    val config = KafkaConfig.fromProps(props)

    server = TestUtils.createServer(config)
  }

  @After
  override def tearDown() {
    server.shutdown
    CoreUtils.delete(server.config.logDirs)
    super.tearDown()
  }

  /**
   * testCompression
   *
   * Compressed messages should be able to sent and consumed correctly
   */
  @Test
  def testCompression() {

    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, TestUtils.getBrokerListStrFromServers(Seq(server)))
    props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compression)
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, "66000")
    props.put(ProducerConfig.LINGER_MS_CONFIG, "200")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
    var producer = new KafkaProducer[Array[Byte],Array[Byte]](props)
    val consumer = new SimpleConsumer("localhost", TestUtils.boundPort(server), 100, 1024*1024, "")

    try {
      // create topic
      TestUtils.createTopic(zkUtils, topic, 1, 1, List(server))
      val partition = 0

      // prepare the messages
      val messages = for (i <-0 until numRecords)
        yield ("value" + i).getBytes

      // make sure the returned messages are correct
      val now = System.currentTimeMillis()
      val responses = for (message <- messages)
        yield producer.send(new ProducerRecord[Array[Byte],Array[Byte]](topic, null, now, null, message))
      val futures = responses.toList
      for ((future, offset) <- futures zip (0 until numRecords)) {
        assertEquals(offset.toLong, future.get.offset)
      }

      // make sure the fetched message count match
      val fetchResponse = consumer.fetch(new FetchRequestBuilder().addFetch(topic, partition, 0, Int.MaxValue).build())
      val messageSet = fetchResponse.messageSet(topic, partition).iterator.toBuffer
      assertEquals("Should have fetched " + numRecords + " messages", numRecords, messageSet.size)

      var index = 0
      for (message <- messages) {
        assertEquals(new Message(bytes = message, now, Message.MagicValue_V1), messageSet(index).message)
        assertEquals(index.toLong, messageSet(index).offset)
        index += 1
      }
    } finally {
      if (producer != null) {
        producer.close()
        producer = null
      }
      if (consumer != null)
        consumer.close()
    }
  }
}

object ProducerCompressionTest {

  // NOTE: Must return collection of Array[AnyRef] (NOT Array[Any]).
  @Parameters
  def parameters: Collection[Array[String]] = {
    val list = new ArrayList[Array[String]]()
    list.add(Array("none"))
    list.add(Array("gzip"))
    list.add(Array("snappy"))
    list.add(Array("lz4"))
    list
  }
}
