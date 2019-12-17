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
package kafka.log.remote

import java._
import java.io.IOException

import kafka.log.LogSegment
import org.apache.kafka.common.annotation.InterfaceStability
import org.apache.kafka.common.record.FileRecords.TimestampAndOffset
import org.apache.kafka.common.record.Records
import org.apache.kafka.common.{Configurable, TopicPartition}

/**
 * RemoteStorageManager is an interface that allows to plugin different remote storage implementations to copy and
 * retrieve the log segments.
 *
 * All these APIs are still experimental.
 */
@InterfaceStability.Unstable
trait RemoteStorageManager extends Configurable with AutoCloseable {

  /**
   * Earliest log offset if exists for the given topic partition in the remote storage. Return -1 if there are no
   * segments in the remote storage.
   *
   * @param tp
   * @return
   */
  @throws(classOf[IOException])
  def earliestLogOffset(tp: TopicPartition): Long

  /**
   * Copies LogSegment provided by [[RemoteLogManager]] for the given topic partition with the given leader epoch.
   * Returns the RDIs of the remote data. This method is invoked by the leader of topic partition.
   *
   * //todo LogSegment is not public, this will be changed with an interface which provides base and end offset of the
   * segment, log and offset/time indexes.
   *
   * @param topicPartition
   * @param logSegment
   * @return
   */
  @throws(classOf[IOException])
  def copyLogSegment(topicPartition: TopicPartition, logSegment: LogSegment, leaderEpoch: Int): util.List[RemoteLogIndexEntry]

  /**
   * List the remote log segment files of the given topicPartition.
   * The RemoteLogManager of a follower uses this method to find out the remote data for the given topic partition.
   *
   * @return List of remote segments, sorted by baseOffset in ascending order.
   */
  @throws(classOf[IOException])
  def listRemoteSegments(topicPartition: TopicPartition): util.List[RemoteLogSegmentInfo] = {
    listRemoteSegments(topicPartition, 0)
  }

  /**
   * List the remote log segment files of the specified topicPartition with offset >= minOffset.
   * The RLM of a follower uses this method to find out the remote data
   *
   * @param minOffset The minimum offset for a segment to be returned.
   * @return List of remote segments that contains offsets >= minOffset, sorted by baseOffset in ascending order.
   */
  @throws(classOf[IOException])
  def listRemoteSegments(topicPartition: TopicPartition, minOffset: Long): util.List[RemoteLogSegmentInfo]

  /**
   * Returns a List of RemoteLogIndexEntry for the given RemoteLogSegmentInfo. This is used by follower to store remote
   * log indexes locally.
   *
   * @param remoteLogSegment
   * @return
   */
  @throws(classOf[IOException])
  def getRemoteLogIndexEntries(remoteLogSegment: RemoteLogSegmentInfo): util.List[RemoteLogIndexEntry]

  /**
   * Deletes remote LogSegment file and indexes for the given remoteLogSegmentInfo
   *
   * @param remoteLogSegmentInfo
   * @return
   */
  @throws(classOf[IOException])
  def deleteLogSegment(remoteLogSegmentInfo: RemoteLogSegmentInfo): Boolean

  /**
   * Delete all the log segments for the given topic partition. This can be done by rename the existing locations
   * and delete them later in asynchronous manner.
   *
   * @param topicPartition
   * @return
   */
  @throws(classOf[IOException])
  def deleteTopicPartition(topicPartition: TopicPartition): Boolean

  /**
   * Remove the log segments which are older than the given cleanUpTillMs. Return the log start offset of the
   * earliest remote log segment if exists or -1 if there are no log segments in the remote storage.
   *
   * @param topicPartition
   * @param cleanUpTillMs
   * @return
   */
  @throws(classOf[IOException])
  def cleanupLogUntil(topicPartition: TopicPartition, cleanUpTillMs: Long): Long

  /**
   * Read up to maxBytes data from remote storage, starting from the 1st batch that is greater than or equals to the
   * startOffset. It will read at least one batch, if the 1st batch size is larger than maxBytes.
   *
   * @param remoteLogIndexEntry The first remoteLogIndexEntry that remoteLogIndexEntry.lastOffset >= startOffset
   * @param maxBytes            maximum bytes to fetch for the given entry
   * @param startOffset         initial offset to be read from the given rdi in remoteLogIndexEntry
   * @param minOneMessage       if true read at least one record even if the size is more than maxBytes
   * @return
   */
  @throws(classOf[IOException])
  def read(remoteLogIndexEntry: RemoteLogIndexEntry, maxBytes: Int, startOffset: Long, minOneMessage: Boolean): Records


  /**
   * Search forward for the first message that meets the following requirements:
   * - Message's timestamp is greater than or equals to the targetTimestamp.
   * - Message's offset is greater than or equals to the startingOffset.
   *
   * @param targetTimestamp The timestamp to search for.
   * @param startingOffset  The starting offset to search.
   * @return The timestamp and offset of the message found. Null if no message is found.
   */
  @throws(classOf[IOException])
  def findOffsetByTimestamp(remoteLogIndexEntry: RemoteLogIndexEntry,
                            targetTimestamp: Long,
                            startingOffset: Long): TimestampAndOffset

  /**
   * Release any system resources used by this instance.
   */
  def close(): Unit
}
