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

package kafka.common

case class OffsetMetadata(offset: Long, metadata: String = OffsetMetadata.NoMetadata) {
  override def toString = "OffsetMetadata[%d,%s]"
    .format(offset,
    if (metadata != null && metadata.length > 0) metadata else "NO_METADATA")
}

object OffsetMetadata {
  val InvalidOffset: Long = -1L
  val NoMetadata: String = ""

  val InvalidOffsetMetadata = OffsetMetadata(OffsetMetadata.InvalidOffset, OffsetMetadata.NoMetadata)
}

case class OffsetAndMetadata(offsetMetadata: OffsetMetadata,
                             commitTimestamp: Long = org.apache.kafka.common.requests.OffsetCommitRequest.DEFAULT_TIMESTAMP,
                             expireTimestamp: Long = org.apache.kafka.common.requests.OffsetCommitRequest.DEFAULT_TIMESTAMP) {

  def offset() = offsetMetadata.offset

  def metadata() = offsetMetadata.metadata

  override def toString = "[%s,CommitTime %d,ExpirationTime %d]".format(offsetMetadata, commitTimestamp, expireTimestamp)
}

object OffsetAndMetadata {
  def apply(offset: Long, metadata: String, commitTimestamp: Long, expireTimestamp: Long) = new OffsetAndMetadata(OffsetMetadata(offset, metadata), commitTimestamp, expireTimestamp)

  def apply(offset: Long, metadata: String, timestamp: Long) = new OffsetAndMetadata(OffsetMetadata(offset, metadata), timestamp)

  def apply(offset: Long, metadata: String) = new OffsetAndMetadata(OffsetMetadata(offset, metadata))

  def apply(offset: Long) = new OffsetAndMetadata(OffsetMetadata(offset, OffsetMetadata.NoMetadata))
}

case class OffsetMetadataAndError(offsetMetadata: OffsetMetadata, error: Short = ErrorMapping.NoError) {
  def offset = offsetMetadata.offset

  def metadata = offsetMetadata.metadata

  override def toString = "[%s,ErrorCode %d]".format(offsetMetadata, error)
}

object OffsetMetadataAndError {
  val NoOffset = OffsetMetadataAndError(OffsetMetadata.InvalidOffsetMetadata, ErrorMapping.NoError)
  val OffsetsLoading = OffsetMetadataAndError(OffsetMetadata.InvalidOffsetMetadata, ErrorMapping.OffsetsLoadInProgressCode)
  val UnknownTopicOrPartition = OffsetMetadataAndError(OffsetMetadata.InvalidOffsetMetadata, ErrorMapping.UnknownTopicOrPartitionCode)
  val NotOffsetManagerForGroup = OffsetMetadataAndError(OffsetMetadata.InvalidOffsetMetadata, ErrorMapping.NotCoordinatorForConsumerCode)

  def apply(offset: Long) = new OffsetMetadataAndError(OffsetMetadata(offset, OffsetMetadata.NoMetadata), ErrorMapping.NoError)

  def apply(error: Short) = new OffsetMetadataAndError(OffsetMetadata.InvalidOffsetMetadata, error)

  def apply(offset: Long, metadata: String, error: Short) = new OffsetMetadataAndError(OffsetMetadata(offset, metadata), error)
}



