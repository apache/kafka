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

import java.io.PrintWriter

import com.yammer.metrics.Metrics
import com.yammer.metrics.core.Gauge
import kafka.utils.{MockTime, TestUtils}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS
import org.apache.kafka.common.record.{CompressionType, RecordBatch}
import org.junit.Assert.{assertFalse, assertTrue, fail}
import org.junit.Test

import scala.collection.JavaConverters.mapAsScalaMapConverter

/**
  * This is an integration test that tests the fully integrated log cleaner
  */
class LogCleanerIntegrationTest extends AbstractLogCleanerIntegrationTest {

  val codec: CompressionType = CompressionType.LZ4

  val time = new MockTime()
  val topicPartitions = Array(new TopicPartition("log", 0), new TopicPartition("log", 1), new TopicPartition("log", 2))

  @Test(timeout = DEFAULT_MAX_WAIT_MS)
  def testMarksPartitionsAsOfflineAndPopulatesUncleanableMetrics() {
    val largeMessageKey = 20
    val (_, largeMessageSet) = createLargeSingleMessageSet(largeMessageKey, RecordBatch.CURRENT_MAGIC_VALUE)
    val maxMessageSize = largeMessageSet.sizeInBytes
    cleaner = makeCleaner(partitions = topicPartitions, maxMessageSize = maxMessageSize, backOffMs = 100)

    def breakPartitionLog(tp: TopicPartition): Unit = {
      val log = cleaner.logs.get(tp)
      writeDups(numKeys = 20, numDups = 3, log = log, codec = codec)

      val partitionFile = log.logSegments.last.log.file()
      val writer = new PrintWriter(partitionFile)
      writer.write("jogeajgoea")
      writer.close()

      writeDups(numKeys = 20, numDups = 3, log = log, codec = codec)
    }

    def getGauge[T](metricName: String, metricScope: String): Gauge[T] = {
      Metrics.defaultRegistry.allMetrics.asScala
        .filterKeys(k => {
          k.getName.endsWith(metricName) && k.getScope.endsWith(metricScope)
        })
        .headOption
        .getOrElse { fail(s"Unable to find metric $metricName") }
        .asInstanceOf[(Any, Gauge[Any])]
        ._2
        .asInstanceOf[Gauge[T]]
    }

    breakPartitionLog(topicPartitions(0))
    breakPartitionLog(topicPartitions(1))

    cleaner.startup()

    val log = cleaner.logs.get(topicPartitions(0))
    val log2 = cleaner.logs.get(topicPartitions(1))
    val uncleanableDirectory = log.dir.getParent
    val uncleanablePartitionsCountGauge = getGauge[Int]("uncleanable-partitions-count", uncleanableDirectory)
    val uncleanableBytesGauge = getGauge[Long]("uncleanable-bytes", uncleanableDirectory)

    TestUtils.waitUntilTrue(() => uncleanablePartitionsCountGauge.value() == 2, "There should be 2 uncleanable partitions", 2000L)
    val expectedTotalUncleanableBytes = LogCleaner.calculateCleanableBytes(log, 0, log.logSegments.last.baseOffset)._2 +
      LogCleaner.calculateCleanableBytes(log2, 0, log2.logSegments.last.baseOffset)._2
    TestUtils.waitUntilTrue(() => uncleanableBytesGauge.value() == expectedTotalUncleanableBytes,
      s"There should be $expectedTotalUncleanableBytes uncleanable bytes", 1000L)

    val uncleanablePartitions = cleaner.cleanerManager.uncleanablePartitions(uncleanableDirectory)
    assertTrue(uncleanablePartitions.contains(topicPartitions(0)))
    assertTrue(uncleanablePartitions.contains(topicPartitions(1)))
    assertFalse(uncleanablePartitions.contains(topicPartitions(2)))
  }
}
