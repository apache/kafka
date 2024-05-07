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

import kafka.server.BrokerTopicStats
import kafka.utils._
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.record.{CompressionType, MemoryRecords, RecordBatch, SimpleRecord}
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.coordinator.transaction.TransactionLogConfigs
import org.apache.kafka.server.record.BrokerCompressionType
import org.apache.kafka.server.util.MockTime
import org.apache.kafka.storage.internals.log.{FetchIsolation, LogConfig, LogDirFailureChannel, ProducerStateManagerConfig}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api._
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, MethodSource}

import java.util.Properties

class BrokerCompressionTest {

  val tmpDir = TestUtils.tempDir()
  val logDir = TestUtils.randomPartitionLogDir(tmpDir)
  val time = new MockTime(0, 0)
  val logConfig = new LogConfig(new Properties)

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
    val messageCompressionType = CompressionType.forName(messageCompression)
    val logProps = new Properties()
    logProps.put(TopicConfig.COMPRESSION_TYPE_CONFIG, brokerCompression)
    /*configure broker-side compression  */
    val log = UnifiedLog(
      dir = logDir,
      config = new LogConfig(logProps),
      logStartOffset = 0L,
      recoveryPoint = 0L,
      scheduler = time.scheduler,
      time = time,
      brokerTopicStats = new BrokerTopicStats,
      maxTransactionTimeoutMs = 5 * 60 * 1000,
      producerStateManagerConfig = new ProducerStateManagerConfig(TransactionLogConfigs.PRODUCER_ID_EXPIRATION_MS_DEFAULT, false),
      producerIdExpirationCheckIntervalMs = TransactionLogConfigs.PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS_DEFAULT,
      logDirFailureChannel = new LogDirFailureChannel(10),
      topicId = None,
      keepPartitionMetadataFile = true
    )

    /* append two messages */
    log.appendAsLeader(MemoryRecords.withRecords(messageCompressionType, 0,
          new SimpleRecord("hello".getBytes), new SimpleRecord("there".getBytes)), leaderEpoch = 0)

    def readBatch(offset: Int): RecordBatch = {
      val fetchInfo = log.read(offset,
        maxLength = 4096,
        isolation = FetchIsolation.LOG_END,
        minOneMessage = true)
      fetchInfo.records.batches.iterator.next()
    }

    if (!brokerCompression.equals("producer")) {
      val brokerCompressionType = BrokerCompressionType.forName(brokerCompression).targetCompressionType(null)
      assertEquals(brokerCompressionType, readBatch(0).compressionType, "Compression at offset 0 should produce " + brokerCompressionType)
    }
    else
      assertEquals(messageCompressionType, readBatch(0).compressionType, "Compression at offset 0 should produce " + messageCompressionType)
  }

}

object BrokerCompressionTest {
  def parameters: java.util.stream.Stream[Arguments] = {
    java.util.Arrays.stream(
      for (brokerCompression <- BrokerCompressionType.values;
           messageCompression <- CompressionType.values
      ) yield Arguments.of(messageCompression.name, brokerCompression.name)
    )
  }
}
