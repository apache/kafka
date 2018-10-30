package kafka.common

import kafka.log.LogSegment

class LogSegmentInvalidOffsetException(val segment: LogSegment, val offset: Long)
  extends org.apache.kafka.common.KafkaException(s"Attempt to append invalid offset $offset to segment $segment") {
}
