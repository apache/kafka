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
import kafka.api.ApiUtils._
import kafka.utils.Logging
import collection.mutable.ListBuffer
import kafka.common.{KafkaException, ErrorMapping}

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

object TopicMetadata {

  def readFrom(buffer: ByteBuffer): TopicMetadata = {
    val errorCode = readShortInRange(buffer, "error code", (-1, Short.MaxValue))
    val topic = readShortString(buffer)
    val numPartitions = readIntInRange(buffer, "number of partitions", (0, Int.MaxValue))
    val partitionsMetadata = new ListBuffer[PartitionMetadata]()
    for(i <- 0 until numPartitions)
      partitionsMetadata += PartitionMetadata.readFrom(buffer)
    new TopicMetadata(topic, partitionsMetadata, errorCode)
  }
}

case class TopicMetadata(topic: String, partitionsMetadata: Seq[PartitionMetadata], errorCode: Short = ErrorMapping.NoError) extends Logging {
  def sizeInBytes: Int = {
    var size: Int = 2   /* error code */
    size += shortStringLength(topic)
    size += partitionsMetadata.foldLeft(4 /* number of partitions */)(_ + _.sizeInBytes)
    debug("Size of topic metadata = " + size)
    size
  }

  def writeTo(buffer: ByteBuffer) {
    /* error code */
    buffer.putShort(errorCode)
    /* topic */
    writeShortString(buffer, topic)
    /* number of partitions */
    buffer.putInt(partitionsMetadata.size)
    partitionsMetadata.foreach(m => m.writeTo(buffer))
  }
}

object PartitionMetadata {

  def readFrom(buffer: ByteBuffer): PartitionMetadata = {
    val errorCode = readShortInRange(buffer, "error code", (-1, Short.MaxValue))
    val partitionId = readIntInRange(buffer, "partition id", (0, Int.MaxValue)) /* partition id */
    val doesLeaderExist = getLeaderRequest(buffer.get)
    val leader = doesLeaderExist match {
      case LeaderExists => /* leader exists */
        Some(Broker.readFrom(buffer))
      case LeaderDoesNotExist => None
    }

    /* list of all replicas */
    val numReplicas = readShortInRange(buffer, "number of all replicas", (0, Short.MaxValue))
    val replicas = new Array[Broker](numReplicas)
    for(i <- 0 until numReplicas) {
      replicas(i) = Broker.readFrom(buffer)
    }

    /* list of in-sync replicas */
    val numIsr = readShortInRange(buffer, "number of in-sync replicas", (0, Short.MaxValue))
    val isr = new Array[Broker](numIsr)
    for(i <- 0 until numIsr) {
      isr(i) = Broker.readFrom(buffer)
    }

    new PartitionMetadata(partitionId, leader, replicas, isr, errorCode)
  }

  private def getLeaderRequest(requestId: Byte): LeaderRequest = {
    requestId match {
      case LeaderExists.requestId => LeaderExists
      case LeaderDoesNotExist.requestId => LeaderDoesNotExist
      case _ => throw new KafkaException("Unknown leader request id " + requestId)
    }
  }
}

case class PartitionMetadata(partitionId: Int, 
                             val leader: Option[Broker], 
                             replicas: Seq[Broker], 
                             isr: Seq[Broker] = Seq.empty,
                             errorCode: Short = ErrorMapping.NoError) extends Logging {
  def sizeInBytes: Int = {
    var size: Int = 2 /* error code */ + 4 /* partition id */ + 1 /* if leader exists*/

    leader match {
      case Some(l) => size += l.sizeInBytes
      case None =>
    }

    size += 2 /* number of replicas */
    size += replicas.foldLeft(0)(_ + _.sizeInBytes)
    size += 2 /* number of in sync replicas */
    size += isr.foldLeft(0)(_ + _.sizeInBytes)

    debug("Size of partition metadata = " + size)
    size
  }

  def writeTo(buffer: ByteBuffer) {
    buffer.putShort(errorCode)
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
  }
}


