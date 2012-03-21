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

import org.scalatest.junit.JUnit3Suite
import kafka.zk.ZooKeeperTestHarness
import kafka.consumer.SimpleConsumer
import org.I0Itec.zkclient.ZkClient
import kafka.server.{KafkaConfig, KafkaRequestHandler, KafkaServer}
import java.util.Properties
import org.apache.log4j.{Level, Logger}
import org.junit.Test
import kafka.utils.{TestZKUtils, Utils, TestUtils}
import kafka.message.Message
import kafka.admin.CreateTopicCommand
import kafka.api.FetchRequestBuilder
import org.junit.Assert._

class ProducerTest extends JUnit3Suite with ZooKeeperTestHarness {
  private val brokerId1 = 0
  private val brokerId2 = 1  
  private val ports = TestUtils.choosePorts(2)
  private val (port1, port2) = (ports(0), ports(1))
  private var server1: KafkaServer = null
  private var server2: KafkaServer = null
  private var consumer1: SimpleConsumer = null
  private var consumer2: SimpleConsumer = null
  private val requestHandlerLogger = Logger.getLogger(classOf[KafkaRequestHandler])
  private var zkClient: ZkClient = null

  override def setUp() {
    super.setUp()
    // set up 2 brokers with 4 partitions each
    zkClient = zookeeper.client

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

  override def tearDown() {
    // restore set request handler logger to a higher level
    requestHandlerLogger.setLevel(Level.ERROR)
    server1.shutdown
    server2.shutdown
    Utils.rm(server1.config.logDir)
    Utils.rm(server2.config.logDir)    
    Thread.sleep(500)
    super.tearDown()
  }

  @Test
  def testZKSendToNewTopic() {
    val props = new Properties()
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("partitioner.class", "kafka.utils.StaticPartitioner")
    props.put("zk.connect", TestZKUtils.zookeeperConnect)

    val config = new ProducerConfig(props)

    val producer = new Producer[String, String](config)
    try {
      // Available partition ids should be 0, 1, 2 and 3. The data in both cases should get sent to partition 0, but
      // since partition 0 can exist on any of the two brokers, we need to fetch from both brokers
      producer.send(new ProducerData[String, String]("new-topic", "test", Array("test1")))
      Thread.sleep(100)
      producer.send(new ProducerData[String, String]("new-topic", "test", Array("test1")))
      Thread.sleep(1000)
      // cross check if one of the brokers got the messages
      val response1 = consumer1.fetch(new FetchRequestBuilder().addFetch("new-topic", 0, 0, 10000).build())
      val messageSet1 = response1.messageSet("new-topic", 0).iterator
      val response2 = consumer2.fetch(new FetchRequestBuilder().addFetch("new-topic", 0, 0, 10000).build())
      val messageSet2 = response2.messageSet("new-topic", 0).iterator
      assertTrue("Message set should have 1 message", (messageSet1.hasNext || messageSet2.hasNext))

      if(messageSet1.hasNext) {
        assertEquals(new Message("test1".getBytes), messageSet1.next.message)
        assertTrue("Message set should have 1 message", messageSet1.hasNext)
        assertEquals(new Message("test1".getBytes), messageSet1.next.message)
      }
      else {
        assertEquals(new Message("test1".getBytes), messageSet2.next.message)
        assertTrue("Message set should have 1 message", messageSet2.hasNext)
        assertEquals(new Message("test1".getBytes), messageSet2.next.message)
      }
    } catch {
      case e: Exception => fail("Not expected", e)
    }
    producer.close
  }

//  @Test
//  def testZKSendWithDeadBroker() {
//    val props = new Properties()
//    props.put("serializer.class", "kafka.serializer.StringEncoder")
//    props.put("partitioner.class", "kafka.utils.StaticPartitioner")
//    props.put("zk.connect", TestZKUtils.zookeeperConnect)
//
//    // create topic
//    CreateTopicCommand.createTopic(zkClient, "new-topic", 2, 1, "0,0")
//
//    val config = new ProducerConfig(props)
//
//    val producer = new Producer[String, String](config)
//    val message = new Message("test1".getBytes)
//    try {
////      // kill 2nd broker
////      server1.shutdown
////      Thread.sleep(100)
//
//      // Available partition ids should be 0, 1, 2 and 3. The data in both cases should get sent to partition 0 and
//      // all partitions have broker 0 as the leader.
//      producer.send(new ProducerData[String, String]("new-topic", "test", Array("test1")))
//      Thread.sleep(100)
//
//      producer.send(new ProducerData[String, String]("new-topic", "test", Array("test1")))
//      Thread.sleep(3000)
//
//      // restart server 1
////      server1.startup()
////      Thread.sleep(100)
//
//      // cross check if brokers got the messages
//      val response = consumer1.fetch(new FetchRequestBuilder().addFetch("new-topic", 0, 0, 10000).build())
//      val messageSet = response.messageSet("new-topic", 0).iterator
//      var numMessagesReceived = 0
//      while(messageSet.hasNext) {
//        val messageAndOffset = messageSet.next()
//        assertEquals(message, messageSet.next.message)
//        println("Received message at offset %d".format(messageAndOffset.offset))
//        numMessagesReceived += 1
//      }
//      assertEquals("Message set should have 2 messages", 2, numMessagesReceived)
//    } catch {
//      case e: Exception => fail("Not expected", e)
//    }
//    producer.close
//  }

  // TODO: Need to rewrite when SyncProducer changes to throw timeout exceptions
  //       and when leader logic is changed.
//  @Test
//  def testZKSendWithDeadBroker2() {
//    val props = new Properties()
//    props.put("serializer.class", "kafka.serializer.StringEncoder")
//    props.put("partitioner.class", "kafka.utils.StaticPartitioner")
//    props.put("socket.timeout.ms", "200")
//    props.put("zk.connect", TestZKUtils.zookeeperConnect)
//
//    // create topic
//    CreateTopicCommand.createTopic(zkClient, "new-topic", 4, 2, "0:1,0:1,0:1,0:1")
//
//    val config = new ProducerConfig(props)
//
//    val producer = new Producer[String, String](config)
//    try {
//      // Available partition ids should be 0, 1, 2 and 3. The data in both cases should get sent to partition 0 and
//      // all partitions have broker 0 as the leader.
//      producer.send(new ProducerData[String, String]("new-topic", "test", Array("test1")))
//      Thread.sleep(100)
//      // kill 2nd broker
//      server1.shutdown
//      Thread.sleep(500)
//
//      // Since all partitions are unavailable, this request will be dropped
//      try {
//        producer.send(new ProducerData[String, String]("new-topic", "test", Array("test1")))
//        fail("Leader broker for \"new-topic\" isn't up, should not be able to send data")
//      } catch {
//        case e: kafka.common.FailedToSendMessageException => // success
//        case e => fail("Leader broker for \"new-topic\" isn't up, should not be able to send data")
//      }
//
//      // restart server 1
//      server1.startup()
//      Thread.sleep(200)
//
//      // cross check if brokers got the messages
//      val response1 = consumer1.fetch(new FetchRequestBuilder().addFetch("new-topic", 0, 0, 10000).build())
//      val messageSet1 = response1.messageSet("new-topic", 0).iterator
//      assertTrue("Message set should have 1 message", messageSet1.hasNext)
//      assertEquals(new Message("test1".getBytes), messageSet1.next.message)
//      assertFalse("Message set should not have more than 1 message", messageSet1.hasNext)
//    } catch {
//      case e: Exception => fail("Not expected", e)
//    }
//    producer.close
//  }

  @Test
  def testZKSendToExistingTopicWithNoBrokers() {
    val props = new Properties()
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("partitioner.class", "kafka.utils.StaticPartitioner")
    props.put("zk.connect", TestZKUtils.zookeeperConnect)

    val config = new ProducerConfig(props)

    val producer = new Producer[String, String](config)
    var server: KafkaServer = null

    // create topic
    CreateTopicCommand.createTopic(zkClient, "new-topic", 4, 2, "0:1,0:1,0:1,0:1")

    try {
      // Available partition ids should be 0, 1, 2 and 3. The data in both cases should get sent to partition 0 and
      // all partitions have broker 0 as the leader.
      producer.send(new ProducerData[String, String]("new-topic", "test", Array("test")))
      Thread.sleep(100)
      // cross check if brokers got the messages
      val response1 = consumer1.fetch(new FetchRequestBuilder().addFetch("new-topic", 0, 0, 10000).build())
      val messageSet1 = response1.messageSet("new-topic", 0).iterator
      assertTrue("Message set should have 1 message", messageSet1.hasNext)
      assertEquals(new Message("test".getBytes), messageSet1.next.message)

      // shutdown server2
      server2.shutdown
      Thread.sleep(100)
      // delete the new-topic logs
      Utils.rm(server2.config.logDir)
      Thread.sleep(100)
      // start it up again. So broker 2 exists under /broker/ids, but nothing exists under /broker/topics/new-topic
      val props2 = TestUtils.createBrokerConfig(brokerId2, port2)
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
      val messageSet2 = response2.messageSet("new-topic", 0).iterator
      assertTrue("Message set should have 1 message", messageSet2.hasNext)
      assertEquals(new Message("test".getBytes), messageSet2.next.message)

    } catch {
      case e: Exception => fail("Not expected", e)
    } finally {
      if(server != null) server.shutdown
      producer.close
    }
  }
}

