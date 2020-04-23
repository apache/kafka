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
package kafka.raft

import java.lang
import java.util.{Optional, OptionalLong}

import kafka.log.Log
import kafka.server.{FetchHighWatermark, FetchLogEnd}
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.record.{MemoryRecords, Records}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.raft
import org.apache.kafka.raft.ReplicatedLog

import scala.compat.java8.OptionConverters._

class KafkaMetadataLog(time: Time, log: Log, maxFetchSizeInBytes: Int = 1024 * 1024) extends ReplicatedLog {

  override def read(startOffset: Long, endOffsetExclusive: OptionalLong): Records = {
    val isolation = if (endOffsetExclusive.isPresent)
      FetchHighWatermark
    else
      FetchLogEnd

    val fetchInfo = log.read(startOffset,
      maxLength = maxFetchSizeInBytes,
      isolation = isolation,
      minOneMessage = true)
    fetchInfo.records
  }

  override def appendAsLeader(records: Records, epoch: Int): lang.Long = {
    val appendInfo = log.appendAsLeader(records.asInstanceOf[MemoryRecords], leaderEpoch = epoch)
    appendInfo.firstOffset.getOrElse {
      throw new KafkaException("Append failed unexpectedly")
    }
  }

  override def appendAsFollower(records: Records): Unit = {
    log.appendAsFollower(records.asInstanceOf[MemoryRecords])
  }

  override def lastFetchedEpoch: Int = {
    log.latestEpoch.getOrElse(0)
  }

  override def endOffsetForEpoch(leaderEpoch: Int): Optional[raft.OffsetAndEpoch] = {
    // TODO: Does this handle empty log case (when epoch is None) as we expect?
    val endOffsetOpt = log.endOffsetForEpoch(leaderEpoch).map { offsetAndEpoch =>
      new raft.OffsetAndEpoch(offsetAndEpoch.offset, offsetAndEpoch.leaderEpoch)
    }
    endOffsetOpt.asJava
  }

  override def endOffset: Long = {
    log.logEndOffset
  }

  override def startOffset: Long = {
    log.logStartOffset
  }

  override def truncateTo(offset: Long): Boolean = {
    log.truncateTo(offset)
  }

  override def previousEpoch: Optional[Integer] = {
    log.previousEpoch.map(_.asInstanceOf[Integer]).asJava
  }

  override def assignEpochStartOffset(epoch: Int, startOffset: Long): Unit = {
    log.maybeAssignEpochStartOffset(epoch, startOffset)
  }

  override def updateHighWatermark(offset: Long): Unit = {
    log.updateHighWatermark(offset)
  }

}
