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

import kafka.message._
import kafka.server.{BrokerTopicStats, FetchLogEnd, LogDirFailureChannel}
import kafka.utils._
import org.apache.kafka.common.record.{CompressionType, MemoryRecords, RecordBatch, SimpleRecord}
import org.apache.kafka.common.utils.Utils
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api._
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, MethodSource}

import java.util.Properties
import scala.jdk.CollectionConverters._

class BrokerCompressionTest {

  val tmpDir = TestUtils.tempDir()
  val logDir = TestUtils.randomPartitionLogDir(tmpDir)
  val time = new MockTime(0, 0)
  val logConfig = LogConfig()

  @AfterEach
  def tearDown(): Unit = {
    Utils.delete(tmpDir)
  }

  /**
   * Test broker-side compression configuration
   */
  @ParameterizedTest
  @MethodSource(Array("parameters"))
  def testBrokerSideCompression(messageCompression: String, brokerCompression: String): Unit = {
    val messageCompressionCode = CompressionCodec.getCompressionCodec(messageCompression)
    val logProps = new Properties()
    logProps.put(LogConfig.CompressionTypeProp, brokerCompression)
    /*configure broker-side compression  */
    val log = Log(logDir, LogConfig(logProps), logStartOffset = 0L, recoveryPoint = 0L, scheduler = time.scheduler,
      time = time, brokerTopicStats = new BrokerTopicStats, maxProducerIdExpirationMs = 60 * 60 * 1000,
      producerIdExpirationCheckIntervalMs = LogManager.ProducerIdExpirationCheckIntervalMs,
      logDirFailureChannel = new LogDirFailureChannel(10), topicId = None, keepPartitionMetadataFile = true)

    /* append two messages */
    log.appendAsLeader(MemoryRecords.withRecords(CompressionType.forId(messageCompressionCode.codec), 0,
          new SimpleRecord("hello".getBytes), new SimpleRecord("there".getBytes)), leaderEpoch = 0)

    def readBatch(offset: Int): RecordBatch = {
      val fetchInfo = log.read(offset,
        maxLength = 4096,
        isolation = FetchLogEnd,
        minOneMessage = true)
      fetchInfo.records.batches.iterator.next()
    }

    if (!brokerCompression.equals("producer")) {
      val brokerCompressionCode = BrokerCompressionCodec.getCompressionCodec(brokerCompression)
      assertEquals(brokerCompressionCode.codec, readBatch(0).compressionType.id, "Compression at offset 0 should produce " + brokerCompressionCode.name)
    }
    else
      assertEquals(messageCompressionCode.codec, readBatch(0).compressionType.id, "Compression at offset 0 should produce " + messageCompressionCode.name)
  }

}

object BrokerCompressionTest {
  def parameters: java.util.stream.Stream[Arguments] = {
    (for (brokerCompression <- BrokerCompressionCodec.brokerCompressionOptions;
         messageCompression <- CompressionType.values
    ) yield Arguments.of(messageCompression.name, brokerCompression)).asJava.stream()
  }
}
