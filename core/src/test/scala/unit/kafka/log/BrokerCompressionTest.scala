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

import kafka.utils._
import kafka.message._
import org.junit._
import org.junit.Assert._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.runners.Parameterized.Parameters
import org.apache.kafka.common.record.{CompressionType, MemoryRecords, RecordBatch, SimpleRecord}
import org.apache.kafka.common.utils.Utils
import java.util.{Collection, Properties}

import kafka.server.{BrokerTopicStats, LogDirFailureChannel}

import scala.collection.JavaConverters._

@RunWith(value = classOf[Parameterized])
class BrokerCompressionTest(messageCompression: String, brokerCompression: String) {

  val tmpDir = TestUtils.tempDir()
  val logDir = TestUtils.randomPartitionLogDir(tmpDir)
  val time = new MockTime(0, 0)
  val logConfig = LogConfig()

  @After
  def tearDown() {
    Utils.delete(tmpDir)
  }

  /**
   * Test broker-side compression configuration
   */
  @Test
  def testBrokerSideCompression() {
    val messageCompressionCode = CompressionCodec.getCompressionCodec(messageCompression)
    val logProps = new Properties()
    logProps.put(LogConfig.CompressionTypeProp, brokerCompression)
    /*configure broker-side compression  */
    val log = Log(logDir, LogConfig(logProps), logStartOffset = 0L, recoveryPoint = 0L, scheduler = time.scheduler,
      time = time, brokerTopicStats = new BrokerTopicStats, maxProducerIdExpirationMs = 60 * 60 * 1000,
      producerIdExpirationCheckIntervalMs = LogManager.ProducerIdExpirationCheckIntervalMs,
      logDirFailureChannel = new LogDirFailureChannel(10))

    /* append two messages */
    log.appendAsLeader(MemoryRecords.withRecords(CompressionType.forId(messageCompressionCode.codec), 0,
          new SimpleRecord("hello".getBytes), new SimpleRecord("there".getBytes)), leaderEpoch = 0)

    def readBatch(offset: Int): RecordBatch = {
      val fetchInfo = log.read(offset, 4096, maxOffset = None,
        includeAbortedTxns = false, minOneMessage = true)
      fetchInfo.records.batches.iterator.next()
    }

    if (!brokerCompression.equals("producer")) {
      val brokerCompressionCode = BrokerCompressionCodec.getCompressionCodec(brokerCompression)
      assertEquals("Compression at offset 0 should produce " + brokerCompressionCode.name, brokerCompressionCode.codec, readBatch(0).compressionType.id)
    }
    else
      assertEquals("Compression at offset 0 should produce " + messageCompressionCode.name, messageCompressionCode.codec, readBatch(0).compressionType.id)
  }

}

object BrokerCompressionTest {
  @Parameters
  def parameters: Collection[Array[String]] = {
    (for (brokerCompression <- BrokerCompressionCodec.brokerCompressionOptions;
         messageCompression <- CompressionType.values
    ) yield Array(messageCompression.name, brokerCompression)).asJava
  }
}
