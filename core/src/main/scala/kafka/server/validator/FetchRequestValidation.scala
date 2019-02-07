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

package kafka.server.validator

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.BaseRecords
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.record.Records
import org.apache.kafka.common.requests.FetchRequest
import org.apache.kafka.common.requests.FetchResponse

final case class FetchRequestValidation(
  erroneous: Vector[FetchRequestValidation.ErrorElem],
  interesting: Vector[FetchRequestValidation.ValidElem]
)

final object FetchRequestValidation {
  type ErrorElem = (TopicPartition, FetchResponse.PartitionData[Records])
  type ValidElem = (TopicPartition, FetchRequest.PartitionData)

  def errorResponse[T >: MemoryRecords <: BaseRecords](error: Errors): FetchResponse.PartitionData[T] = {
    new FetchResponse.PartitionData[T](error,
      FetchResponse.INVALID_HIGHWATERMARK,
      FetchResponse.INVALID_LAST_STABLE_OFFSET,
      FetchResponse.INVALID_LOG_START_OFFSET,
      null,
      MemoryRecords.EMPTY)
  }
}
