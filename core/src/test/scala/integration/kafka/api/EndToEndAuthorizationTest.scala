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

import java.io.File
import java.util.ArrayList
import java.util.concurrent.ExecutionException

import kafka.admin.AclCommand
import kafka.common.{TopicAndPartition}
import kafka.security.auth._
import kafka.server._
import kafka.utils._

import org.apache.kafka.clients.consumer.{Consumer, ConsumerRecord, ConsumerConfig}
import org.apache.kafka.clients.producer.{ProducerRecord, ProducerConfig}
import org.apache.kafka.common.{TopicPartition}
import org.apache.kafka.common.protocol.SecurityProtocol
import org.apache.kafka.common.errors.TopicAuthorizationException
import org.junit.Assert._
import org.junit.{Test, After, Before}

import scala.collection.JavaConverters._


/**
  * The test cases here verify that a producer authorized to publish to a topic
  * is able to, and that consumers in a group authorized to consume are able to
  * to do so.
  *
  * This test relies on a chain of test harness traits to set up. It directly
  * extends IntegrationTestHarness. IntegrationTestHarness creates producers and
  * consumers, and it extends KafkaServerTestHarness. KafkaServerTestHarness starts
  * brokers, but first it initializes a ZooKeeper server and client, which happens
  * in ZooKeeperTestHarness.
  *
  * To start brokers when the security protocol is SASL_SSL, we need to set a cluster
  * ACL, which happens optionally in KafkaServerTestHarness. If the security protocol
  * is SSL or PLAINTEXT, then the ACL isn't set. The remaining ACLs to enable access
  * to producers and consumers are set here. To set ACLs, we use AclCommand directly.
  *
  * Finally, we rely on SaslSetup to bootstrap and setup Kerberos. We don't use
  * SaslTestHarness here directly because it extends ZooKeeperTestHarness, and we
  * would end up with ZooKeeperTestHarness.
  */
class EndToEndAuthorizationTest extends IntegrationTestHarness with SaslSetup {
  override val producerCount = 1
  override val consumerCount = 2
  override val serverCount = 3
  val numRecords = 1
  val group = "group"
  val topic = "e2etopic"
  val part = 0
  val tp = new TopicPartition(topic, part)
  val topicAndPartition = new TopicAndPartition(topic, part)

  override protected val zkSaslEnabled = true
  override protected def securityProtocol = SecurityProtocol.SASL_SSL
  override protected lazy val trustStoreFile = Some(File.createTempFile("truststore", ".jks"))

  val topicResource = new Resource(Topic, topic)
  val groupResource = new Resource(Group, group)

  // Arguments to AclCommand to set ACLs. There are three definitions here:
  // 1- Provides read and write access to topic
  // 2- Provides only write access to topic
  // 3- Provides read access to consumer group
  def topicAclArgs: Array[String] = Array("--authorizer-properties", 
                                          s"zookeeper.connect=$zkConnect",
                                          s"--add",
                                          s"--topic=$topic",
                                          s"--operation=Read",
                                          s"--operation=Write",
                                          s"--allow-principal=$clientPrincipal")
  def topicWriteAclArgs: Array[String] = Array("--authorizer-properties", 
                                               s"zookeeper.connect=$zkConnect",
                                               s"--add",
                                               s"--topic=$topic",
                                               s"--operation=Write",
                                               s"--allow-principal=$clientPrincipal")
  def groupAclArgs: Array[String] = Array("--authorizer-properties", 
                                          s"zookeeper.connect=$zkConnect",
                                          s"--add",
                                          s"--group=$group",
                                          s"--operation=Read",
                                          s"--allow-principal=$clientPrincipal")
  // The next two configuration parameters enable ZooKeeper secure ACLs
  // and sets the Kafka authorizer, both necessary to enable security.
  this.serverConfig.setProperty(KafkaConfig.ZkEnableSecureAclsProp, "true")
  this.serverConfig.setProperty(KafkaConfig.AuthorizerClassNameProp, classOf[SimpleAclAuthorizer].getName)
  // Some needed configuration for brokers, producers, and consumers
  this.serverConfig.setProperty(KafkaConfig.OffsetsTopicPartitionsProp, "1")
  this.serverConfig.setProperty(KafkaConfig.OffsetsTopicReplicationFactorProp, "1")
  this.producerConfig.setProperty(ProducerConfig.ACKS_CONFIG, "1")
  this.consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group")

  /**
    * Starts MiniKDC and only then sets up the parent trait.
    */
  @Before
  override def setUp {
    startSasl()
    super.setUp
    // create the test topic with all the brokers as replicas
    TestUtils.createTopic(zkUtils, topic, 1, 1, this.servers)
  }

  /**
    * Closes MiniKDC last when tearing down.
    */
  @After
  override def tearDown {
    super.tearDown
    closeSasl()
  }

  /**
    * Tests the ability of producing and consuming with the appropriate ACLs set.
    */
  @Test
  def testProduceConsume {
    AclCommand.main(topicAclArgs)
    AclCommand.main(groupAclArgs)
    //Produce records
    debug("Starting to send records")
    sendRecords(numRecords, tp)
    //Consume records
    debug("Finished sending and starting to consume records")
    consumers.head.assign(List(tp).asJava)
    consumeRecords(this.consumers.head)
    debug("Finished consuming")
  }

  /**
    * Tests that a producer fails to publish messages when the appropriate ACL
    * isn't set.
    */
  @Test
  def testNotProduce {
    //Produce records
    debug("Starting to send records")
    try{
      sendRecords(numRecords, tp)
      fail("Topic authorization exception expected")
    } catch {
      case e: TopicAuthorizationException => //expected
    }
  }

  /**
    * Tests that a consumer fails to consume messages without the appropriate
    * ACL set.
    */
  @Test
  def testNotConsume {
    AclCommand.main(topicWriteAclArgs)
    AclCommand.main(groupAclArgs)
    //Produce records
    debug("Starting to send records")
    sendRecords(numRecords, tp)
    //Consume records
    debug("Finished sending and starting to consume records")
    consumers.head.assign(List(tp).asJava)
    try{
      consumeRecords(this.consumers.head)
      fail("Topic authorization exception expected")
    } catch {
      case e: TopicAuthorizationException => //expected
    }
  }
  
  private def sendRecords(numRecords: Int, tp: TopicPartition) {
    val futures = (0 until numRecords).map { i =>
      val record = new ProducerRecord(tp.topic(), tp.partition(), s"$i".getBytes, s"$i".getBytes)
      debug(s"Sending this record: $record")
      this.producers.head.send(record)
    }
    try {
      futures.foreach(_.get)
    } catch {
      case e: ExecutionException => throw e.getCause
    }
  }

  private def consumeRecords(consumer: Consumer[Array[Byte], Array[Byte]],
                             numRecords: Int = 1,
                             startingOffset: Int = 0,
                             topic: String = topic,
                             part: Int = part) {
    val records = new ArrayList[ConsumerRecord[Array[Byte], Array[Byte]]]()
    val maxIters = numRecords * 50
    var iters = 0
    while (records.size < numRecords) {
      for (record <- consumer.poll(50).asScala) {
        records.add(record)
      }
      if (iters > maxIters)
        throw new IllegalStateException("Failed to consume the expected records after " + iters + " iterations.")
      iters += 1
    }
    for (i <- 0 until numRecords) {
      val record = records.get(i)
      val offset = startingOffset + i
      assertEquals(topic, record.topic())
      assertEquals(part, record.partition())
      assertEquals(offset.toLong, record.offset())
    } 
  }
}