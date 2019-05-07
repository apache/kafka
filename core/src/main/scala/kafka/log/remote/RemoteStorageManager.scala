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

import kafka.log.{LogReadInfo, LogSegment}
import org.apache.kafka.common.{Configurable, TopicPartition}

// all these APIs are still experimental, in poc mode.
trait RemoteStorageManager extends Configurable with AutoCloseable {

  /**
   * Copies LogSegment provided by [[RemoteLogManager]]
   * Returns the RDIs of the remote data
   * This method is used by the leader
   *
   * @param logSegment
   * @return
   */
  def copyLogSegment(logSegment: LogSegment): (RDI, Seq[RemoteLogIndexEntry])

  /**
   * Cancels the logsegment that is being copied currently to remote storage
   *
   * @param logSegment
   * @return Returns true if it cancels copying else it returns false
   */
  def cancelCopyingLogSegment(logSegment: LogSegment): Boolean

  /**
   * List the remote log segment files of the specified topicPartition
   * The RLM of a follower uses this method to find out the remote data
   */
  def listRemoteSegments(topicPartition: TopicPartition): Seq[RemoteLogSegmentInfo]

  /**
   * Called by the RLM of a follower to retrieve RemoteLogIndex entries
   * of the new remote log segment
   *
   * @param remoteLogSegment
   * @return
   */
  def getRemoteLogIndexEntries(remoteLogSegment: RemoteLogSegmentInfo): Seq[RemoteLogIndexEntry]

  /**
   * Deletes remote LogSegment file provided by the RLM
   *
   * @param remoteLogSegment
   * @return
   */
  def deleteLogSegment(remoteLogSegment: RemoteLogSegmentInfo): Boolean

  /**
   * Read topic partition data from remote storage, starting from the given offset.
   *
   * @param remoteLocation
   * @param maxBytes
   * @param offset
   * @return
   */
  def read(remoteLocation: RDI, maxBytes: Int, offset: Long): LogReadInfo

  /**
   * stops all the threads and closes the instance.
   */
  def close(): Unit
}
