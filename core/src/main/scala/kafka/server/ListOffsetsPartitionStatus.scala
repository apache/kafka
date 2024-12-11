/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.server

import org.apache.kafka.common.errors.ApiException
import org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsPartitionResponse
import org.apache.kafka.common.record.FileRecords.TimestampAndOffset
import org.apache.kafka.storage.internals.log.AsyncOffsetReadFutureHolder

class ListOffsetsPartitionStatus(val futureHolderOpt: Option[AsyncOffsetReadFutureHolder[Either[Exception, Option[TimestampAndOffset]]]],
                                 val lastFetchableOffset: Option[Long],
                                 val maybeOffsetsError: Option[ApiException]) {

  @volatile var responseOpt: Option[ListOffsetsPartitionResponse] = None
  @volatile var completed = false

  override def toString: String = {
    s"[responseOpt: $responseOpt, lastFetchableOffset: $lastFetchableOffset, " +
      s"maybeOffsetsError: $maybeOffsetsError, completed: $completed]"
  }
}

object ListOffsetsPartitionStatus {
  def apply(responseOpt: Option[ListOffsetsPartitionResponse],
            futureHolderOpt: Option[AsyncOffsetReadFutureHolder[Either[Exception, Option[TimestampAndOffset]]]] = None,
            lastFetchableOffset: Option[Long] = None,
            maybeOffsetsError: Option[ApiException] = None): ListOffsetsPartitionStatus = {
    val status = new ListOffsetsPartitionStatus(futureHolderOpt, lastFetchableOffset, maybeOffsetsError)
    status.responseOpt = responseOpt
    status
  }
}
