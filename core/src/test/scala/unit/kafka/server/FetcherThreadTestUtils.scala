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

import kafka.cluster.BrokerEndPoint
import kafka.server.InitialFetchState
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.record.{CompressionType, MemoryRecords, RecordBatch, SimpleRecord}

import scala.jdk.CollectionConverters._

object FetcherThreadTestUtils {
  def mkBatch(baseOffset: Long, leaderEpoch: Int, records: SimpleRecord*): RecordBatch = {
    MemoryRecords.withRecords(baseOffset, CompressionType.NONE, leaderEpoch, records: _*)
      .batches.asScala.head
  }

  def initialFetchState(topicId: Option[Uuid], fetchOffset: Long, leaderEpoch: Int): InitialFetchState = {
    InitialFetchState(topicId = topicId, leader = new BrokerEndPoint(0, "localhost", 9092),
      initOffset = fetchOffset, currentLeaderEpoch = leaderEpoch)
  }
}
