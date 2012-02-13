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

package kafka.producer

import junit.framework.Assert._
import java.util.Properties
import kafka.api.FetchRequestBuilder
import kafka.consumer.SimpleConsumer
import kafka.message.Message
import kafka.serializer.Encoder
import kafka.server.{KafkaRequestHandler, KafkaServer, KafkaConfig}
import kafka.utils.{TestUtils, TestZKUtils, Utils}
import kafka.zk.EmbeddedZookeeper
import org.apache.log4j.{Logger, Level}
import org.junit.{After, Before, Test}
import org.scalatest.junit.JUnitSuite

class ProducerTest extends JUnitSuite {
  private val topic = "test-topic"
  private val brokerId1 = 0
  private val brokerId2 = 1  
  private val ports = TestUtils.choosePorts(2)
  private val (port1, port2) = (ports(0), ports(1))
  private var server1: KafkaServer = null
  private var server2: KafkaServer = null
  private var consumer1: SimpleConsumer = null
  private var consumer2: SimpleConsumer = null
  private var zkServer:EmbeddedZookeeper = null
  private val requestHandlerLogger = Logger.getLogger(classOf[KafkaRequestHandler])

  @Before
  def setUp() {
    // set up 2 brokers with 4 partitions each
    zkServer = new EmbeddedZookeeper(TestZKUtils.zookeeperConnect)

    val props1 = TestUtils.createBrokerConfig(brokerId1, port1)
    val config1 = new KafkaConfig(props1) {
      override val numPartitions = 4
    }
    server1 = TestUtils.createServer(config1)

    val props2 = TestUtils.createBrokerConfig(brokerId2, port2)
    val config2 = new KafkaConfig(props2) {
      override val numPartitions = 4
    }
    server2 = TestUtils.createServer(config2)

    val props = new Properties()
    props.put("host", "localhost")
    props.put("port", port1.toString)

    consumer1 = new SimpleConsumer("localhost", port1, 1000000, 64*1024)
    consumer2 = new SimpleConsumer("localhost", port2, 100, 64*1024)

    // temporarily set request handler logger to a higher level
    requestHandlerLogger.setLevel(Level.FATAL)

    Thread.sleep(500)
  }

  @After
  def tearDown() {
    // restore set request handler logger to a higher level
    requestHandlerLogger.setLevel(Level.ERROR)
    server1.shutdown
    server2.shutdown
    Utils.rm(server1.config.logDir)
    Utils.rm(server2.config.logDir)    
    Thread.sleep(500)
    zkServer.shutdown
    Thread.sleep(500)
  }

  @Test
  def testZKSendToNewTopic() {
    val props = new Properties()
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("partitioner.class", "kafka.producer.StaticPartitioner")
    props.put("zk.connect", TestZKUtils.zookeeperConnect)

    val config = new ProducerConfig(props)

    val producer = new Producer[String, String](config)
    try {
      // Available broker id, partition id at this stage should be (0,0), (1,0)
      // this should send the message to broker 0 on partition 0
      producer.send(new ProducerData[String, String]("new-topic", "test", Array("test1")))
      Thread.sleep(100)
      // Available broker id, partition id at this stage should be (0,0), (0,1), (0,2), (0,3), (1,0)
      // Since 4 % 5 = 4, this should send the message to broker 1 on partition 0
      producer.send(new ProducerData[String, String]("new-topic", "test", Array("test1")))
      Thread.sleep(100)
      // cross check if brokers got the messages
      val response1 = consumer1.fetch(new FetchRequestBuilder().addFetch("new-topic", 0, 0, 10000).build())
      val messageSet1 = response1.messageSet("new-topic", 0)
      assertTrue("Message set should have 1 message", messageSet1.iterator.hasNext)
      assertEquals(new Message("test1".getBytes), messageSet1.head.message)
      val response2 = consumer2.fetch(new FetchRequestBuilder().addFetch("new-topic", 0, 0, 10000).build())
      val messageSet2 = response2.messageSet("new-topic", 0)
      assertTrue("Message set should have 1 message", messageSet2.iterator.hasNext)
      assertEquals(new Message("test1".getBytes), messageSet2.head.message)
    } catch {
      case e: Exception => fail("Not expected", e)
    }
    producer.close
  }

  @Test
  def testZKSendWithDeadBroker() {
    val props = new Properties()
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("partitioner.class", "kafka.producer.StaticPartitioner")
    props.put("zk.connect", TestZKUtils.zookeeperConnect)

    val config = new ProducerConfig(props)

    val producer = new Producer[String, String](config)
    try {
      // Available broker id, partition id at this stage should be (0,0), (1,0)
      // Hence, this should send the message to broker 0 on partition 0
      producer.send(new ProducerData[String, String]("new-topic", "test", Array("test1")))
      Thread.sleep(100)
      // kill 2nd broker
      server2.shutdown
      Thread.sleep(100)
      // Available broker id, partition id at this stage should be (0,0), (0,1), (0,2), (0,3), (1,0)
      // Since 4 % 5 = 4, in a normal case, it would send to broker 1 on partition 0. But since broker 1 is down,
      // 4 % 4 = 0, So it should send the message to broker 0 on partition 0
      producer.send(new ProducerData[String, String]("new-topic", "test", Array("test1")))
      Thread.sleep(100)
      // cross check if brokers got the messages
      val response1 = consumer1.fetch(new FetchRequestBuilder().addFetch("new-topic", 0, 0, 10000).build())
      val messageSet1Iter = response1.messageSet("new-topic", 0).iterator
      assertTrue("Message set should have 1 message", messageSet1Iter.hasNext)
      assertEquals(new Message("test1".getBytes), messageSet1Iter.next.message)
      assertTrue("Message set should have another message", messageSet1Iter.hasNext)
      assertEquals(new Message("test1".getBytes), messageSet1Iter.next.message)
    } catch {
      case e: Exception => fail("Not expected")
    }
    producer.close
  }

  @Test
  def testZKSendToExistingTopicWithNoBrokers() {
    val props = new Properties()
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("partitioner.class", "kafka.producer.StaticPartitioner")
    props.put("zk.connect", TestZKUtils.zookeeperConnect)

    val config = new ProducerConfig(props)

    val producer = new Producer[String, String](config)
    var server: KafkaServer = null

    try {
      // shutdown server1
      server1.shutdown
      Thread.sleep(100)
      // Available broker id, partition id at this stage should be (1,0)
      // this should send the message to broker 1 on partition 0
      producer.send(new ProducerData[String, String]("new-topic", "test", Array("test")))
      Thread.sleep(100)
      // cross check if brokers got the messages
      val response1 = consumer2.fetch(new FetchRequestBuilder().addFetch("new-topic", 0, 0, 10000).build())
      val messageSet1 = response1.messageSet("new-topic", 0)
      assertTrue("Message set should have 1 message", messageSet1.iterator.hasNext)
      assertEquals(new Message("test".getBytes), messageSet1.head.message)

      // shutdown server2
      server2.shutdown
      Thread.sleep(100)
      // delete the new-topic logs
      Utils.rm(server2.config.logDir)
      Thread.sleep(100)
      // start it up again. So broker 2 exists under /broker/ids, but nothing exists under /broker/topics/new-topic
      val props2 = TestUtils.createBrokerConfig(brokerId1, port1)
      val config2 = new KafkaConfig(props2) {
        override val numPartitions = 4
      }
      server = TestUtils.createServer(config2)
      Thread.sleep(100)

      // now there are no brokers registered under test-topic.
      producer.send(new ProducerData[String, String]("new-topic", "test", Array("test")))
      Thread.sleep(100)

      // cross check if brokers got the messages
      val response2 = consumer1.fetch(new FetchRequestBuilder().addFetch("new-topic", 0, 0, 10000).build())
      val messageSet2 = response2.messageSet("new-topic", 0)
      assertTrue("Message set should have 1 message", messageSet2.iterator.hasNext)
      assertEquals(new Message("test".getBytes), messageSet2.head.message)

    } catch {
      case e: Exception => fail("Not expected", e)
    }finally {
      server.shutdown
      producer.close
    }
  }

}

class StringSerializer extends Encoder[String] {
  def toEvent(message: Message):String = message.toString
  def toMessage(event: String):Message = new Message(event.getBytes)
  def getTopic(event: String): String = event.concat("-topic")
}

class NegativePartitioner extends Partitioner[String] {
  def partition(data: String, numPartitions: Int): Int = {
    -1
  }
}

class StaticPartitioner extends Partitioner[String] {
  def partition(data: String, numPartitions: Int): Int = {
    (data.length % numPartitions)
  }
}

class HashPartitioner extends Partitioner[String] {
  def partition(data: String, numPartitions: Int): Int = {
    (data.hashCode % numPartitions)
  }
}
