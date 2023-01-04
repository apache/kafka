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

package kafka.server

import kafka.api.Request
import org.apache.kafka.common.IsolationLevel
import org.apache.kafka.common.message.FetchResponseData
import org.apache.kafka.common.record.{MemoryRecords, Records}
import org.apache.kafka.common.replica.ClientMetadata
import org.apache.kafka.common.requests.FetchRequest
import org.apache.kafka.server.log.internals.LogOffsetMetadata

sealed trait FetchIsolation
case object FetchLogEnd extends FetchIsolation
case object FetchHighWatermark extends FetchIsolation
case object FetchTxnCommitted extends FetchIsolation

object FetchIsolation {
  def apply(
    request: FetchRequest
  ): FetchIsolation = {
    apply(request.replicaId, request.isolationLevel)
  }

  def apply(
    replicaId: Int,
    isolationLevel: IsolationLevel
  ): FetchIsolation = {
    if (!Request.isConsumer(replicaId))
      FetchLogEnd
    else if (isolationLevel == IsolationLevel.READ_COMMITTED)
      FetchTxnCommitted
    else
      FetchHighWatermark
  }
}

case class FetchParams(
  requestVersion: Short,
  replicaId: Int,
  maxWaitMs: Long,
  minBytes: Int,
  maxBytes: Int,
  isolation: FetchIsolation,
  clientMetadata: Option[ClientMetadata]
) {
  def isFromFollower: Boolean = Request.isValidBrokerId(replicaId)
  def isFromConsumer: Boolean = Request.isConsumer(replicaId)
  def fetchOnlyLeader: Boolean = isFromFollower || (isFromConsumer && clientMetadata.isEmpty)
  def hardMaxBytesLimit: Boolean = requestVersion <= 2

  override def toString: String = {
    s"FetchParams(requestVersion=$requestVersion" +
      s", replicaId=$replicaId" +
      s", maxWaitMs=$maxWaitMs" +
      s", minBytes=$minBytes" +
      s", maxBytes=$maxBytes" +
      s", isolation=$isolation" +
      s", clientMetadata= $clientMetadata" +
      ")"
  }
}

object FetchDataInfo {
  def empty(fetchOffset: Long): FetchDataInfo = {
    FetchDataInfo(
      fetchOffsetMetadata = new LogOffsetMetadata(fetchOffset),
      records = MemoryRecords.EMPTY,
      firstEntryIncomplete = false,
      abortedTransactions = None
    )
  }
}

case class FetchDataInfo(
  fetchOffsetMetadata: LogOffsetMetadata,
  records: Records,
  firstEntryIncomplete: Boolean = false,
  abortedTransactions: Option[List[FetchResponseData.AbortedTransaction]] = None
)
