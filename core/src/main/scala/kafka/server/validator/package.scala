package kafka.server

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.BaseRecords
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.record.Records
import org.apache.kafka.common.requests.FetchRequest
import org.apache.kafka.common.requests.FetchResponse
import scala.collection.mutable

package object validator {
  type ErrorElem = (TopicPartition, FetchResponse.PartitionData[Records])
  type ValidElem = (TopicPartition, FetchRequest.PartitionData)

  type FetchPartitionValidator = RequestValidator[ValidElem, Option[ErrorElem]]
  type FetchRequestValidator = RequestValidator[(FetchRequest, mutable.Map[TopicPartition, FetchRequest.PartitionData]), Vector[ErrorElem]]

  def errorResponse[T >: MemoryRecords <: BaseRecords](error: Errors): FetchResponse.PartitionData[T] = {
    new FetchResponse.PartitionData[T](error,
      FetchResponse.INVALID_HIGHWATERMARK,
      FetchResponse.INVALID_LAST_STABLE_OFFSET,
      FetchResponse.INVALID_LOG_START_OFFSET,
      null,
      MemoryRecords.EMPTY)
  }
}
