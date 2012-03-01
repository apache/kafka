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
package kafka.server

import java.io.File
import kafka.consumer.SimpleConsumer
import java.util.Properties
import org.junit.Test
import junit.framework.Assert._
import kafka.message.{Message, ByteBufferMessageSet}
import org.scalatest.junit.JUnit3Suite
import kafka.zk.ZooKeeperTestHarness
import kafka.utils.{TestUtils, Utils}
import kafka.producer._
import kafka.admin.CreateTopicCommand
import kafka.api.FetchRequestBuilder

class ServerShutdownTest extends JUnit3Suite with ZooKeeperTestHarness {
  val port = TestUtils.choosePort

  @Test
  def testCleanShutdown() {
    val props = TestUtils.createBrokerConfig(0, port)
    val config = new KafkaConfig(props)

    val host = "localhost"
    val topic = "test"
    val sent1 = List(new Message("hello".getBytes()), new Message("there".getBytes()))
    val sent2 = List( new Message("more".getBytes()), new Message("messages".getBytes()))

    {
      val server = new KafkaServer(config)
      server.startup()

      // create topic
      CreateTopicCommand.createTopic(zookeeper.client, topic, 1, 1, "0")

      val producer = new Producer[Int, Message](getProducerConfig(64*1024, 100000, 10000))

      // send some messages
      producer.send(new ProducerData[Int, Message](topic, 0, sent1))

      Thread.sleep(200)
      // do a clean shutdown
      server.shutdown()
      val cleanShutDownFile = new File(new File(config.logDir), server.CleanShutdownFile)
      assertTrue(cleanShutDownFile.exists)
    }


    {
      val producer = new Producer[Int, Message](getProducerConfig(64*1024, 100000, 10000))
      val consumer = new SimpleConsumer(host,
                                        port,
                                        1000000,
                                        64*1024)

      val server = new KafkaServer(config)
      server.startup()

      Thread.sleep(100)

      var fetchedMessage: ByteBufferMessageSet = null
      while(fetchedMessage == null || fetchedMessage.validBytes == 0) {
        val fetched = consumer.fetch(new FetchRequestBuilder().addFetch(topic, 0, 0, 10000).build())
        fetchedMessage = fetched.messageSet(topic, 0)
      }
      TestUtils.checkEquals(sent1.iterator, fetchedMessage.map(m => m.message).iterator)
      val newOffset = fetchedMessage.validBytes

      // send some more messages
      println("Sending messages to topic " + topic)
      producer.send(new ProducerData[Int, Message](topic, 0, sent2))

      Thread.sleep(200)

      fetchedMessage = null
      while(fetchedMessage == null || fetchedMessage.validBytes == 0) {
        val fetched = consumer.fetch(new FetchRequestBuilder().addFetch(topic, 0, newOffset, 10000).build())
        fetchedMessage = fetched.messageSet(topic, 0)
      }
      TestUtils.checkEquals(sent2.iterator, fetchedMessage.map(m => m.message).iterator)

      server.shutdown()
      Utils.rm(server.config.logDir)
    }

  }

  private def getProducerConfig(bufferSize: Int, connectTimeout: Int,
                                reconnectInterval: Int): ProducerConfig = {
    val props = new Properties()
    props.put("zk.connect", zkConnect)
    props.put("partitioner.class", "kafka.utils.FixedValuePartitioner")
    props.put("buffer.size", bufferSize.toString)
    props.put("connect.timeout.ms", connectTimeout.toString)
    props.put("reconnect.interval", reconnectInterval.toString)
    new ProducerConfig(props)
  }
}
