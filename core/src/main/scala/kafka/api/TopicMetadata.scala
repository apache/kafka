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

package kafka.api

import kafka.cluster.Broker
import java.nio.ByteBuffer
import kafka.utils.Utils._
import collection.mutable.ListBuffer

/**
 * topic (2 bytes + topic.length)
 * number of partitions (4 bytes)
 *
 * partition id (4 bytes)
 *
 * does leader exist (1 byte)
 * leader info (4 + creator.length + host.length + 4 (port) + 4 (id))
 * number of replicas (2 bytes)
 * replica info (4 + creator.length + host.length + 4 (port) + 4 (id))
 * number of in sync replicas (2 bytes)
 * replica info (4 + creator.length + host.length + 4 (port) + 4 (id))
 *
 * does log metadata exist (1 byte)
 * number of log segments (4 bytes)
 * total size of log in bytes (8 bytes)
 *
 * number of log segments (4 bytes)
 * beginning offset (8 bytes)
 * last modified timestamp (8 bytes)
 * size of log segment (8 bytes)
 *
 */

sealed trait LeaderRequest { def requestId: Byte }
case object LeaderExists extends LeaderRequest { val requestId: Byte = 1 }
case object LeaderDoesNotExist extends LeaderRequest { val requestId: Byte = 0 }

sealed trait LogSegmentMetadataRequest { def requestId: Byte }
case object LogSegmentMetadataExists extends LogSegmentMetadataRequest { val requestId: Byte = 1 }
case object LogSegmentMetadataDoesNotExist extends LogSegmentMetadataRequest { val requestId: Byte = 0 }

object TopicMetadata {

  def readFrom(buffer: ByteBuffer): TopicMetadata = {
    val topic = readShortString(buffer)
    val numPartitions = getIntInRange(buffer, "number of partitions", (0, Int.MaxValue))
    val partitionsMetadata = new ListBuffer[PartitionMetadata]()
    for(i <- 0 until numPartitions)
      partitionsMetadata += PartitionMetadata.readFrom(buffer)
    new TopicMetadata(topic, partitionsMetadata)
  }
}

case class TopicMetadata(topic: String, partitionsMetadata: Seq[PartitionMetadata]) {
  def sizeInBytes: Int = {
    var size: Int = shortStringLength(topic)
    size += partitionsMetadata.foldLeft(4 /* number of partitions */)(_ + _.sizeInBytes)
    debug("Size of topic metadata = " + size)
    size
  }

  def writeTo(buffer: ByteBuffer) {
    /* topic */
    writeShortString(buffer, topic)
    /* number of partitions */
    buffer.putInt(partitionsMetadata.size)
    partitionsMetadata.foreach(m => m.writeTo(buffer))
  }
}

object PartitionMetadata {

  def readFrom(buffer: ByteBuffer): PartitionMetadata = {
    val partitionId = getIntInRange(buffer, "partition id", (0, Int.MaxValue)) /* partition id */
    val doesLeaderExist = getLeaderRequest(buffer.get)
    val leader = doesLeaderExist match {
      case LeaderExists => /* leader exists */
        Some(Broker.readFrom(buffer))
      case LeaderDoesNotExist => None
    }

    /* list of all replicas */
    val numReplicas = getShortInRange(buffer, "number of all replicas", (0, Short.MaxValue))
    val replicas = new Array[Broker](numReplicas)
    for(i <- 0 until numReplicas) {
      replicas(i) = Broker.readFrom(buffer)
    }

    /* list of in-sync replicas */
    val numIsr = getShortInRange(buffer, "number of in-sync replicas", (0, Short.MaxValue))
    val isr = new Array[Broker](numIsr)
    for(i <- 0 until numIsr) {
      isr(i) = Broker.readFrom(buffer)
    }

    val doesLogMetadataExist = getLogSegmentMetadataRequest(buffer.get)
    val logMetadata = doesLogMetadataExist match {
      case LogSegmentMetadataExists =>
        val numLogSegments = getIntInRange(buffer, "total number of log segments", (0, Int.MaxValue))
        val totalDataSize = getLongInRange(buffer, "total data size", (0, Long.MaxValue))
        val numSegmentMetadata = getIntInRange(buffer, "number of log segment metadata", (0, Int.MaxValue))
        val segmentMetadata = numSegmentMetadata match {
          case 0 => None
          case _ =>
            val metadata = new ListBuffer[LogSegmentMetadata]()
            for(i <- 0 until numSegmentMetadata) {
              val beginningOffset = getLongInRange(buffer, "beginning offset", (0, Long.MaxValue))
              val lastModified = getLongInRange(buffer, "last modified time", (0, Long.MaxValue))
              val size = getLongInRange(buffer, "size of log segment", (0, Long.MaxValue))
              metadata += new LogSegmentMetadata(beginningOffset, lastModified, size)
            }
            Some(metadata)
        }
        Some(new LogMetadata(numLogSegments, totalDataSize, segmentMetadata))
      case LogSegmentMetadataDoesNotExist => None
    }
    new PartitionMetadata(partitionId, leader, replicas, isr, logMetadata)
  }

  def getLeaderRequest(requestId: Byte): LeaderRequest = {
    requestId match {
      case LeaderExists.requestId => LeaderExists
      case LeaderDoesNotExist.requestId => LeaderDoesNotExist
      case _ => throw new IllegalArgumentException("Unknown leader request id " + requestId)
    }
  }

  def getLogSegmentMetadataRequest(requestId: Byte): LogSegmentMetadataRequest = {
    requestId match {
      case LogSegmentMetadataExists.requestId => LogSegmentMetadataExists
      case LogSegmentMetadataDoesNotExist.requestId => LogSegmentMetadataDoesNotExist
    }
  }
}

case class PartitionMetadata(partitionId: Int, leader: Option[Broker], replicas: Seq[Broker], isr: Seq[Broker] = Seq.empty,
                             logMetadata: Option[LogMetadata] = None) {
  def sizeInBytes: Int = {
    var size: Int = 4 /* partition id */ + 1 /* if leader exists*/

    leader match {
      case Some(l) => size += l.sizeInBytes
      case None =>
    }

    size += 2 /* number of replicas */
    size += replicas.foldLeft(0)(_ + _.sizeInBytes)
    size += 2 /* number of in sync replicas */
    size += isr.foldLeft(0)(_ + _.sizeInBytes)

    size += 1 /* if log segment metadata exists */
    logMetadata match {
      case Some(metadata) => size += metadata.sizeInBytes
      case None =>
    }
    debug("Size of partition metadata = " + size)
    size
  }

  def writeTo(buffer: ByteBuffer) {
    buffer.putInt(partitionId)

    /* if leader exists*/
    leader match {
      case Some(l) =>
        buffer.put(LeaderExists.requestId)
        /* leader id host_name port */
        l.writeTo(buffer)
      case None => buffer.put(LeaderDoesNotExist.requestId)
    }

    /* number of replicas */
    buffer.putShort(replicas.size.toShort)
    replicas.foreach(r => r.writeTo(buffer))

    /* number of in-sync replicas */
    buffer.putShort(isr.size.toShort)
    isr.foreach(r => r.writeTo(buffer))

    /* if log segment metadata exists */
    logMetadata match {
      case Some(metadata) =>
        buffer.put(LogSegmentMetadataExists.requestId)
        metadata.writeTo(buffer)
      case None => buffer.put(LogSegmentMetadataDoesNotExist.requestId)
    }

  }
}

case class LogMetadata(numLogSegments: Int, totalSize: Long, logSegmentMetadata: Option[Seq[LogSegmentMetadata]]) {
  def sizeInBytes: Int = {
    var size: Int = 4 /* num log segments */ + 8 /* total data size */ + 4 /* number of log segment metadata */
    logSegmentMetadata match {
      case Some(segmentMetadata) => size += segmentMetadata.foldLeft(0)(_ + _.sizeInBytes)
      case None =>
    }
    debug("Size of log metadata = " + size)
    size
  }

  def writeTo(buffer: ByteBuffer) {
    buffer.putInt(numLogSegments)
    buffer.putLong(totalSize)
    /* if segment metadata exists */
    logSegmentMetadata match {
      case Some(segmentMetadata) =>
        /* number of log segments */
        buffer.putInt(segmentMetadata.size)
        segmentMetadata.foreach(m => m.writeTo(buffer))
      case None =>
        buffer.putInt(0)
    }
  }
}

case class LogSegmentMetadata(beginningOffset: Long, lastModified: Long, size: Long) {
  def sizeInBytes: Int = {
    8 /* beginning offset */ + 8 /* last modified timestamp */ + 8 /* log segment size in bytes */
  }

  def writeTo(buffer: ByteBuffer) {
    buffer.putLong(beginningOffset)
    buffer.putLong(lastModified)
    buffer.putLong(size)
  }
}


