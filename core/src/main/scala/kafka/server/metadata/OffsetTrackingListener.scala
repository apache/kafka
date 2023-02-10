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

package kafka.server.metadata

import org.apache.kafka.raft.{BatchReader, RaftClient}
import org.apache.kafka.server.common.ApiMessageAndVersion
import org.apache.kafka.snapshot.SnapshotReader

/**
 *  A simple Raft listener that only keeps track of the highest offset seen. Used for registration of ZK
 *  brokers with the KRaft controller during a KIP-866 migration.
 */
class OffsetTrackingListener extends RaftClient.Listener[ApiMessageAndVersion] {
  @volatile var _highestOffset = 0L

  def highestOffset: Long = _highestOffset

  override def handleCommit(reader: BatchReader[ApiMessageAndVersion]): Unit = {
    reader.lastOffset()
    var index = 0
    while (reader.hasNext) {
      index += 1
      reader.next()
    }
    _highestOffset = reader.lastOffset().orElse(reader.baseOffset() + index)
    reader.close()
  }

  override def handleSnapshot(reader: SnapshotReader[ApiMessageAndVersion]): Unit = {
    _highestOffset = reader.lastContainedLogOffset()
    reader.close()
  }
}
