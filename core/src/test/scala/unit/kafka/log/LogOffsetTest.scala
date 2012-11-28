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

package kafka.log

import java.io.File
import kafka.utils._
import kafka.server.{KafkaConfig, KafkaServer}
import junit.framework.Assert._
import java.util.{Random, Properties}
import kafka.api.{FetchRequest, OffsetRequest}
import collection.mutable.WrappedArray
import kafka.consumer.SimpleConsumer
import org.scalatest.junit.JUnitSuite
import org.junit.{After, Before, Test}
import kafka.message.{NoCompressionCodec, ByteBufferMessageSet, Message}
import org.apache.log4j._

object LogOffsetTest {
  val random = new Random()  
}

class LogOffsetTest extends JUnitSuite {
  var logDir: File = null
  var topicLogDir: File = null
  var server: KafkaServer = null
  var logSize: Int = 100
  val brokerPort: Int = 9099
  var simpleConsumer: SimpleConsumer = null

  @Before
  def setUp() {
    val config: Properties = createBrokerConfig(1, brokerPort)
    val logDirPath = config.getProperty("log.dir")
    logDir = new File(logDirPath)
    
    server = TestUtils.createServer(new KafkaConfig(config))
    simpleConsumer = new SimpleConsumer("localhost", brokerPort, 1000000, 64*1024)
  }

  @After
  def tearDown() {
    simpleConsumer.close
    server.shutdown
    Utils.rm(logDir)
  }

  @Test
  def testEmptyLogs() {
    val messageSet: ByteBufferMessageSet = simpleConsumer.fetch(
      new FetchRequest("test", 0, 0, 300 * 1024))
    assertFalse(messageSet.iterator.hasNext)

    val name = "test"
    val logFile = new File(logDir, name + "-0")
    
    {
      val offsets = simpleConsumer.getOffsetsBefore(name, 0, OffsetRequest.LatestTime, 10)
      assertTrue( (Array(0L): WrappedArray[Long]) == (offsets: WrappedArray[Long]) )
      assertTrue(!logFile.exists())
    }

    {
      val offsets = simpleConsumer.getOffsetsBefore(name, 0, OffsetRequest.EarliestTime, 10)
      assertTrue( (Array(0L): WrappedArray[Long]) == (offsets: WrappedArray[Long]) )
      assertTrue(!logFile.exists())
    }

    {
      val offsets = simpleConsumer.getOffsetsBefore(name, 0, SystemTime.milliseconds, 10)
      assertEquals( 0, offsets.length )
      assertTrue(!logFile.exists())
    }
  }

  @Test
  def testGetOffsetsBeforeLatestTime() {
    val topicPartition = "kafka-" + 0
    val topicPartitionPath = getLogDir.getAbsolutePath + "/" + topicPartition
    val topic = topicPartition.split("-").head
    val part = Integer.valueOf(topicPartition.split("-").last).intValue

    val logManager = server.getLogManager
    val log = logManager.getOrCreateLog(topic, part)

    val message = new Message(Integer.toString(42).getBytes())
    for(i <- 0 until 20)
      log.append(new ByteBufferMessageSet(NoCompressionCodec, message))
    log.flush()

    Thread.sleep(100)

    val offsetRequest = new OffsetRequest(topic, part, OffsetRequest.LatestTime, 10)

    val offsets = log.getOffsetsBefore(offsetRequest)
    assertTrue((Array(240L, 216L, 108L, 0L): WrappedArray[Long]) == (offsets: WrappedArray[Long]))

    val consumerOffsets = simpleConsumer.getOffsetsBefore(topic, part,
                                                          OffsetRequest.LatestTime, 10)
    assertTrue((Array(240L, 216L, 108L, 0L): WrappedArray[Long]) == (consumerOffsets: WrappedArray[Long]))

    // try to fetch using latest offset
    val messageSet: ByteBufferMessageSet = simpleConsumer.fetch(
      new FetchRequest(topic, 0, consumerOffsets.head, 300 * 1024))
    assertFalse(messageSet.iterator.hasNext)
  }

  @Test
  def testEmptyLogsGetOffsets() {
    val topicPartition = "kafka-" + LogOffsetTest.random.nextInt(10)
    val topicPartitionPath = getLogDir.getAbsolutePath + "/" + topicPartition
    topicLogDir = new File(topicPartitionPath)
    topicLogDir.mkdir

    val topic = topicPartition.split("-").head
    val part = Integer.valueOf(topicPartition.split("-").last).intValue

    var offsetChanged = false
    for(i <- 1 to 14) {
      val consumerOffsets = simpleConsumer.getOffsetsBefore(topic, part,
        OffsetRequest.EarliestTime, 1)

      if(consumerOffsets(0) == 1) {
        offsetChanged = true
      }
    }
    assertFalse(offsetChanged)
  }

  @Test
  def testGetOffsetsBeforeNow() {
    val topicPartition = "kafka-" + LogOffsetTest.random.nextInt(10)
    val topicPartitionPath = getLogDir.getAbsolutePath + "/" + topicPartition
    val topic = topicPartition.split("-").head
    val part = Integer.valueOf(topicPartition.split("-").last).intValue

    val logManager = server.getLogManager
    val log = logManager.getOrCreateLog(topic, part)
    val message = new Message(Integer.toString(42).getBytes())
    for(i <- 0 until 20)
      log.append(new ByteBufferMessageSet(NoCompressionCodec, message))
    log.flush()

    val now = System.currentTimeMillis
    Thread.sleep(100)

    val offsetRequest = new OffsetRequest(topic, part, now, 10)
    val offsets = log.getOffsetsBefore(offsetRequest)
    assertTrue((Array(216L, 108L, 0L): WrappedArray[Long]) == (offsets: WrappedArray[Long]))

    val consumerOffsets = simpleConsumer.getOffsetsBefore(topic, part, now, 10)
    assertTrue((Array(216L, 108L, 0L): WrappedArray[Long]) == (consumerOffsets: WrappedArray[Long]))
  }

  @Test
  def testGetOffsetsBeforeEarliestTime() {
    val topicPartition = "kafka-" + LogOffsetTest.random.nextInt(10)
    val topicPartitionPath = getLogDir.getAbsolutePath + "/" + topicPartition
    val topic = topicPartition.split("-").head
    val part = Integer.valueOf(topicPartition.split("-").last).intValue

    val logManager = server.getLogManager
    val log = logManager.getOrCreateLog(topic, part)
    val message = new Message(Integer.toString(42).getBytes())
    for(i <- 0 until 20)
      log.append(new ByteBufferMessageSet(NoCompressionCodec, message))
    log.flush()

    Thread.sleep(100)

    val offsetRequest = new OffsetRequest(topic, part,
                                          OffsetRequest.EarliestTime, 10)
    val offsets = log.getOffsetsBefore(offsetRequest)

    assertTrue( (Array(0L): WrappedArray[Long]) == (offsets: WrappedArray[Long]) )

    val consumerOffsets = simpleConsumer.getOffsetsBefore(topic, part,
                                                          OffsetRequest.EarliestTime, 10)
    assertTrue( (Array(0L): WrappedArray[Long]) == (offsets: WrappedArray[Long]) )
  }

  private def createBrokerConfig(nodeId: Int, port: Int): Properties = {
    val props = new Properties
    props.put("brokerid", nodeId.toString)
    props.put("port", port.toString)
    props.put("log.dir", getLogDir.getAbsolutePath)
    props.put("log.flush.interval", "1")
    props.put("enable.zookeeper", "false")
    props.put("num.partitions", "20")
    props.put("log.retention.hours", "10")
    props.put("log.cleanup.interval.mins", "5")
    props.put("log.file.size", logSize.toString)
    props
  }

  private def getLogDir(): File = {
    val dir = TestUtils.tempDir()
    dir
  }

}
