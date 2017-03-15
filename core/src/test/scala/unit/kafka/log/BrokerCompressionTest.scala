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
import org.scalatest.junit.JUnitSuite
import org.junit._
import org.junit.Assert._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.runners.Parameterized.Parameters
import org.apache.kafka.common.record.{CompressionType, MemoryRecords, Record}
import org.apache.kafka.common.utils.Utils
import java.util.{Collection, Properties}

import scala.collection.JavaConverters._

@RunWith(value = classOf[Parameterized])
class BrokerCompressionTest(messageCompression: String, brokerCompression: String) extends JUnitSuite {

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
    val log = new Log(logDir, LogConfig(logProps), recoveryPoint = 0L, time.scheduler, time = time)

    /* append two messages */
    log.append(MemoryRecords.withRecords(CompressionType.forId(messageCompressionCode.codec),
      Record.create("hello".getBytes), Record.create("there".getBytes)))

    def readMessage(offset: Int) = log.read(offset, 4096).records.shallowEntries.iterator.next().record

    if (!brokerCompression.equals("producer")) {
      val brokerCompressionCode = BrokerCompressionCodec.getCompressionCodec(brokerCompression)
      assertEquals("Compression at offset 0 should produce " + brokerCompressionCode.name, brokerCompressionCode.codec, readMessage(0).compressionType.id)
    }
    else
      assertEquals("Compression at offset 0 should produce " + messageCompressionCode.name, messageCompressionCode.codec, readMessage(0).compressionType.id)
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
