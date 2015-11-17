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
import java.util.Arrays
import java.util.concurrent.ExecutionException
import java.util.Properties

import kafka.admin.AclCommand
import kafka.cluster.EndPoint
import kafka.common.{ErrorMapping, TopicAndPartition, KafkaException}
import kafka.coordinator.GroupCoordinator
import kafka.integration.{KafkaServerTestHarness, SecurityTestHarness}
import kafka.security.auth._
import kafka.server._
import kafka.utils._
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.clients.consumer.{OffsetAndMetadata, Consumer, ConsumerRecord, ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import org.apache.kafka.common.{PartitionInfo, TopicPartition}
import org.apache.kafka.common.protocol.SecurityProtocol
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.junit.Assert._
import org.junit.{Test, Before}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.Buffer

class EndToEndAuthorizationTest extends SecurityTestHarness {
  override val producerCount = 1
  override val consumerCount = 2
  override val serverCount = 3
  override val numRecords = 1
  override val group = "group"
  val topic = "topic"
  val part = 0
  val tp = new TopicPartition(topic, part)
  val topicAndPartition = new TopicAndPartition(topic, part)
  
  val topicResource = new Resource(Topic, topic)
  val groupResource = new Resource(Group, group)
  def topicAclArgs: Array[String] = Array("--authorizer-properties", 
                                          s"zookeeper.connect=$zkConnect",
                                          s"--add",
                                          s"--topic=$topic",
                                          s"--operations=Read,Write",
                                          s"--allow-principal=$clientPrincipal")   
  def groupAclArgs: Array[String] = Array("--authorizer-properties", 
                                          s"zookeeper.connect=$zkConnect",
                                          s"--add",
                                          s"--group=$group",
                                          s"--operations=Read",
                                          s"--allow-principal=$clientPrincipal")

  @Before
  override def setUp {
    super.setUp
    // create the test topic with all the brokers as replicas
    TestUtils.createTopic(zkUtils, topic, 1, 1, this.servers)
  }
  
  @Test
  def produceConsumeTest {
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