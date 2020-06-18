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
import java.util.concurrent.ExecutionException

import kafka.log.LogConfig
import kafka.utils.TestUtils
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.errors.{InvalidTimestampException, SerializationException}
import org.apache.kafka.common.record.TimestampType
import org.junit.Assert._
import org.junit.Test

class PlaintextProducerSendTest extends BaseProducerSendTest {

  protected override def propertyOverrides(props: Properties): Unit = {
    props.put("log.preallocate","true")
  }



  @Test(expected = classOf[SerializationException])
  def testWrongSerializer() {
    val producerProps = new Properties()
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    val producer = registerProducer(new KafkaProducer(producerProps))
    val record = new ProducerRecord[Array[Byte], Array[Byte]](topic, 0, "key".getBytes, "value".getBytes)
    producer.send(record)
  }

  @Test
  def testSend() {

    val producer = createProducer(brokerList = brokerList)
    val record = new ProducerRecord[Array[Byte], Array[Byte]](topic, 0, "key".getBytes, "value".getBytes)
    val future = producer.send(record)
    val rec = future.get()
    assert(rec.partition() == 0)
    producer.close()
  }

  @Test
  def testRdmaSend() {

    val producer = createProducer(brokerList = brokerList)
    val record = new ProducerRecord[Array[Byte], Array[Byte]](topic, 0, "key".getBytes, "value".getBytes)
    val future = producer.RDMAsend(record)
    val rec = future.get()
    assert(rec.partition() == 0)
   // producer.close()
  }


  @Test
  def testBatchSizeZero() {
    val producer = createProducer(brokerList = brokerList,
      lingerMs = Int.MaxValue,
      deliveryTimeoutMs = Int.MaxValue,
      batchSize = 0)
    sendAndVerify(producer)
  }

  @Test
  def testSendCompressedMessageWithLogAppendTime() {
    val producer = createProducer(brokerList = brokerList,
      compressionType = "gzip",
      lingerMs = Int.MaxValue,
      deliveryTimeoutMs = Int.MaxValue)
    sendAndVerifyTimestamp(producer, TimestampType.LOG_APPEND_TIME)
  }

  @Test
  def testSendNonCompressedMessageWithLogAppendTime() {
    val producer = createProducer(brokerList = brokerList, lingerMs = Int.MaxValue, deliveryTimeoutMs = Int.MaxValue)
    sendAndVerifyTimestamp(producer, TimestampType.LOG_APPEND_TIME)
  }

  /**
   * testAutoCreateTopic
   *
   * The topic should be created upon sending the first message
   */
  @Test
  def testAutoCreateTopic() {
    val producer = createProducer(brokerList)
    try {
      // Send a message to auto-create the topic
      val record = new ProducerRecord(topic, null, "key".getBytes, "value".getBytes)
      assertEquals("Should have offset 0", 0L, producer.send(record).get.offset)

      // double check that the topic is created with leader elected
      TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, topic, 0)

    } finally {
      producer.close()
    }
  }

  @Test
  def testSendWithInvalidCreateTime() {
    val topicProps = new Properties()
    topicProps.setProperty(LogConfig.MessageTimestampDifferenceMaxMsProp, "1000")
    createTopic(topic, 1, 2, topicProps)

    val producer = createProducer(brokerList = brokerList)
    try {
      producer.send(new ProducerRecord(topic, 0, System.currentTimeMillis() - 1001, "key".getBytes, "value".getBytes)).get()
      fail("Should throw CorruptedRecordException")
    } catch {
      case e: ExecutionException => assertTrue(e.getCause.isInstanceOf[InvalidTimestampException])
    } finally {
      producer.close()
    }

    // Test compressed messages.
    val compressedProducer = createProducer(brokerList = brokerList, compressionType = "gzip")
    try {
      compressedProducer.send(new ProducerRecord(topic, 0, System.currentTimeMillis() - 1001, "key".getBytes, "value".getBytes)).get()
      fail("Should throw CorruptedRecordException")
    } catch {
      case e: ExecutionException => assertTrue(e.getCause.isInstanceOf[InvalidTimestampException])
    } finally {
      compressedProducer.close()
    }
  }

}
