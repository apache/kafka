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

import java.io.IOException
import java._

import kafka.log.LogSegment
import org.apache.kafka.common.record.Records
import org.apache.kafka.common.{Configurable, TopicPartition}

// all these APIs are still experimental, in poc mode.
trait RemoteStorageManager extends Configurable with AutoCloseable {

  /**
   * Copies LogSegment provided by [[RemoteLogManager]]
   * Returns the RDIs of the remote data
   * This method is used by the leader
   *
   * @param topicPartition The topic-partition this LogSegment belongs to
   * @param logSegment
   * @return
   */
  @throws(classOf[IOException])
  def copyLogSegment(topicPartition: TopicPartition, logSegment: LogSegment): util.List[RemoteLogIndexEntry]

  /**
   * Cancels the unfinished LogSegment copying of this given topic-partition
   *
   * @param topicPartition
   */
  def cancelCopyingLogSegment(topicPartition: TopicPartition): Unit

  /**
   * List the remote log segment files of the specified topicPartition
   * The RLM of a follower uses this method to find out the remote data
   *
   * @return List of remote segments, sorted by baseOffset in ascending order.
   */
  @throws(classOf[IOException])
  def listRemoteSegments(topicPartition: TopicPartition): util.List[RemoteLogSegmentInfo] = {
    listRemoteSegments(topicPartition, 0)
  }

  /**
   * List the remote log segment files of the specified topicPartition starting from the base offset minBaseOffset.
   * The RLM of a follower uses this method to find out the remote data
   *
   * @param minBaseOffset The minimum base offset for a segment to be returned.
   * @return List of remote segments starting from the base offset minBaseOffset, sorted by baseOffset in ascending order.
   */
  @throws(classOf[IOException])
  def listRemoteSegments(topicPartition: TopicPartition, minBaseOffset: Long): util.List[RemoteLogSegmentInfo]

  /**
   * Called by the RLM to retrieve the RemoteLogIndex entries of the specified remoteLogSegment.
   *
   * @param remoteLogSegment
   * @return
   */
  @throws(classOf[IOException])
  def getRemoteLogIndexEntries(remoteLogSegment: RemoteLogSegmentInfo): util.List[RemoteLogIndexEntry]

  /**
   * Deletes remote LogSegment file provided by the RLM
   *
   * @param remoteLogSegment
   * @return
   */
  @throws(classOf[IOException])
  def deleteLogSegment(remoteLogSegment: RemoteLogSegmentInfo): Boolean

  /**
   * Read up to maxBytes data from remote storage, starting from the 1st batch that
   * is greater than or equals to the startOffset.
   *
   * Will read at least one batch, if the 1st batch size is larger than maxBytes.
   *
   * @param remoteLogIndexEntry The first remoteLogIndexEntry that remoteLogIndexEntry.lastOffset >= startOffset
   * @param maxBytes maximum bytes to fetch for the given entry
   * @param startOffset initial offset to be read from the given rdi in remoteLogIndexEntry
   * @param minOneMessage if true read at least one record even if the size is more than maxBytes
   * @return
   */
  @throws(classOf[IOException])
  def read(remoteLogIndexEntry: RemoteLogIndexEntry, maxBytes: Int, startOffset: Long, minOneMessage: Boolean): Records

  /**
   * stops all the threads and closes the instance.
   */
  def close(): Unit
}
