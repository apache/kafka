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
import org.junit.Test
import junit.framework.Assert._
import kafka.message.ByteBufferMessageSet
import org.scalatest.junit.JUnit3Suite
import kafka.zk.ZooKeeperTestHarness
import kafka.producer._
import kafka.utils.IntEncoder
import kafka.utils.TestUtils._
import kafka.admin.AdminUtils
import kafka.api.FetchRequestBuilder
import kafka.utils.{TestUtils, Utils}

class ServerShutdownTest extends JUnit3Suite with ZooKeeperTestHarness {
  val port = TestUtils.choosePort
  val props = TestUtils.createBrokerConfig(0, port)
  val config = new KafkaConfig(props)

  val host = "localhost"
  val topic = "test"
  val sent1 = List("hello", "there")
  val sent2 = List("more", "messages")

  @Test
  def testCleanShutdown() {
    var server = new KafkaServer(config)
    server.startup()
    val producerConfig = getProducerConfig(TestUtils.getBrokerListStrFromConfigs(Seq(config)))
    producerConfig.put("key.serializer.class", classOf[IntEncoder].getName.toString)
    var producer = new Producer[Int, String](new ProducerConfig(producerConfig))

    // create topic
    AdminUtils.createTopic(zkClient, topic, 1, 1)
    // send some messages
    producer.send(sent1.map(m => new KeyedMessage[Int, String](topic, 0, m)):_*)

    // do a clean shutdown and check that the clean shudown file is written out
    server.shutdown()
    for(logDir <- config.logDirs) {
      val cleanShutDownFile = new File(logDir, server.logManager.CleanShutdownFile)
      assertTrue(cleanShutDownFile.exists)
    }
    producer.close()
    
    /* now restart the server and check that the written data is still readable and everything still works */
    server = new KafkaServer(config)
    server.startup()

    producer = new Producer[Int, String](new ProducerConfig(producerConfig))
    val consumer = new SimpleConsumer(host, port, 1000000, 64*1024, "")

    waitUntilLeaderIsElectedOrChanged(zkClient, topic, 0, 1000)

    var fetchedMessage: ByteBufferMessageSet = null
    while(fetchedMessage == null || fetchedMessage.validBytes == 0) {
      val fetched = consumer.fetch(new FetchRequestBuilder().addFetch(topic, 0, 0, 10000).maxWait(0).build())
      fetchedMessage = fetched.messageSet(topic, 0)
    }
    assertEquals(sent1, fetchedMessage.map(m => Utils.readString(m.message.payload)))
    val newOffset = fetchedMessage.last.nextOffset

    // send some more messages
    producer.send(sent2.map(m => new KeyedMessage[Int, String](topic, 0, m)):_*)

    fetchedMessage = null
    while(fetchedMessage == null || fetchedMessage.validBytes == 0) {
      val fetched = consumer.fetch(new FetchRequestBuilder().addFetch(topic, 0, newOffset, 10000).build())
      fetchedMessage = fetched.messageSet(topic, 0)
    }
    assertEquals(sent2, fetchedMessage.map(m => Utils.readString(m.message.payload)))

    consumer.close()
    producer.close()
    server.shutdown()
    Utils.rm(server.config.logDirs)
  }
}
