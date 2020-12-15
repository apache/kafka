/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.FetchResponseData
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.{CompressionType, MemoryRecords, Records, SimpleRecord}
import org.apache.kafka.common.requests.FetchResponse
import org.junit.Assert.assertEquals
import org.junit.Test
import org.mockito.{ArgumentMatchers, Mockito}

import java.nio.charset.StandardCharsets
import java.util
import java.util.{Collections, Optional}
import scala.collection.Map
import scala.jdk.CollectionConverters._

class KafkaApisUtilsTest {

  @Test
  def testSizeOfThrottledPartitions(): Unit = {
    def fetchResponse(data: Map[TopicPartition, String]): FetchResponse[Records] = {
      val responseData = new util.LinkedHashMap[TopicPartition, FetchResponse.PartitionData[Records]](data.map {
        case (tp, raw) =>
          tp -> new FetchResponse.PartitionData(Errors.NONE,
            105, 105, 0, Optional.empty(), Collections.emptyList(), Optional.empty(),
            MemoryRecords.withRecords(CompressionType.NONE,
              new SimpleRecord(100, raw.getBytes(StandardCharsets.UTF_8))).asInstanceOf[Records])
      }.toMap.asJava)
      new FetchResponse(Errors.NONE, responseData, 100, 100)
    }

    val throttledPartition = new TopicPartition("throttledData", 0)
    val throttledData = Map(throttledPartition -> "throttledData")
    val expectedSize = FetchResponse.sizeOf(FetchResponseData.HIGHEST_SUPPORTED_VERSION,
      fetchResponse(throttledData).responseData().entrySet().iterator)

    val response = fetchResponse(throttledData ++ Map(new TopicPartition("nonThrottledData", 0) -> "nonThrottledData"))

    val quota = Mockito.mock(classOf[ReplicationQuotaManager])
    Mockito.when(quota.isThrottled(ArgumentMatchers.any(classOf[TopicPartition])))
      .thenAnswer(invocation => throttledPartition == invocation.getArgument(0).asInstanceOf[TopicPartition])

    assertEquals(expectedSize, KafkaApisUtils.sizeOfThrottledPartitions(FetchResponseData.HIGHEST_SUPPORTED_VERSION, response, quota))
  }
}
