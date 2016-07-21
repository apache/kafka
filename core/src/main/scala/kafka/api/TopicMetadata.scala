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

import kafka.cluster.BrokerEndPoint
import java.nio.ByteBuffer
import kafka.api.ApiUtils._
import kafka.utils.Logging
import org.apache.kafka.common.protocol.Errors

object TopicMetadata {

  val NoLeaderNodeId = -1

  def readFrom(buffer: ByteBuffer, brokers: Map[Int, BrokerEndPoint]): TopicMetadata = {
    val errorCode = readShortInRange(buffer, "error code", (-1, Short.MaxValue))
    val topic = readShortString(buffer)
    val numPartitions = readIntInRange(buffer, "number of partitions", (0, Int.MaxValue))
    val partitionsMetadata: Array[PartitionMetadata] = new Array[PartitionMetadata](numPartitions)
    for(i <- 0 until numPartitions) {
      val partitionMetadata = PartitionMetadata.readFrom(buffer, brokers)
      partitionsMetadata(i) = partitionMetadata
    }
    new TopicMetadata(topic, partitionsMetadata, errorCode)
  }
}

case class TopicMetadata(topic: String, partitionsMetadata: Seq[PartitionMetadata], errorCode: Short = Errors.NONE.code) extends Logging {
  def sizeInBytes: Int = {
    2 /* error code */ +
    shortStringLength(topic) +
    4 + partitionsMetadata.map(_.sizeInBytes).sum /* size and partition data array */
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

  override def toString: String = {
    val topicMetadataInfo = new StringBuilder
    topicMetadataInfo.append("{TopicMetadata for topic %s -> ".format(topic))
    Errors.forCode(errorCode) match {
      case Errors.NONE =>
        partitionsMetadata.foreach { partitionMetadata =>
          Errors.forCode(partitionMetadata.errorCode) match {
            case Errors.NONE =>
              topicMetadataInfo.append("\nMetadata for partition [%s,%d] is %s".format(topic,
                partitionMetadata.partitionId, partitionMetadata.toString()))
            case Errors.REPLICA_NOT_AVAILABLE =>
              // this error message means some replica other than the leader is not available. The consumer
              // doesn't care about non leader replicas, so ignore this
              topicMetadataInfo.append("\nMetadata for partition [%s,%d] is %s".format(topic,
                partitionMetadata.partitionId, partitionMetadata.toString()))
            case error: Errors =>
              topicMetadataInfo.append("\nMetadata for partition [%s,%d] is not available due to %s".format(topic,
                partitionMetadata.partitionId, error.exceptionName))
          }
        }
      case error: Errors =>
        topicMetadataInfo.append("\nNo partition metadata for topic %s due to %s".format(topic,
          error.exceptionName))
    }
    topicMetadataInfo.append("}")
    topicMetadataInfo.toString()
  }
}

object PartitionMetadata {

  def readFrom(buffer: ByteBuffer, brokers: Map[Int, BrokerEndPoint]): PartitionMetadata = {
    val errorCode = readShortInRange(buffer, "error code", (-1, Short.MaxValue))
    val partitionId = readIntInRange(buffer, "partition id", (0, Int.MaxValue)) /* partition id */
    val leaderId = buffer.getInt
    val leader = brokers.get(leaderId)

    /* list of all replicas */
    val numReplicas = readIntInRange(buffer, "number of all replicas", (0, Int.MaxValue))
    val replicaIds = (0 until numReplicas).map(_ => buffer.getInt)
    val replicas = replicaIds.map(brokers)

    /* list of in-sync replicas */
    val numIsr = readIntInRange(buffer, "number of in-sync replicas", (0, Int.MaxValue))
    val isrIds = (0 until numIsr).map(_ => buffer.getInt)
    val isr = isrIds.map(brokers)

    new PartitionMetadata(partitionId, leader, replicas, isr, errorCode)
  }
}

case class PartitionMetadata(partitionId: Int,
                             leader: Option[BrokerEndPoint],
                             replicas: Seq[BrokerEndPoint],
                             isr: Seq[BrokerEndPoint] = Seq.empty,
                             errorCode: Short = Errors.NONE.code) extends Logging {
  def sizeInBytes: Int = {
    2 /* error code */ +
    4 /* partition id */ +
    4 /* leader */ +
    4 + 4 * replicas.size /* replica array */ +
    4 + 4 * isr.size /* isr array */
  }

  def writeTo(buffer: ByteBuffer) {
    buffer.putShort(errorCode)
    buffer.putInt(partitionId)

    /* leader */
    val leaderId = if(leader.isDefined) leader.get.id else TopicMetadata.NoLeaderNodeId
    buffer.putInt(leaderId)

    /* number of replicas */
    buffer.putInt(replicas.size)
    replicas.foreach(r => buffer.putInt(r.id))

    /* number of in-sync replicas */
    buffer.putInt(isr.size)
    isr.foreach(r => buffer.putInt(r.id))
  }

  override def toString: String = {
    val partitionMetadataString = new StringBuilder
    partitionMetadataString.append("\tpartition " + partitionId)
    partitionMetadataString.append("\tleader: " + (if(leader.isDefined) leader.get.toString else "none"))
    partitionMetadataString.append("\treplicas: " + replicas.mkString(","))
    partitionMetadataString.append("\tisr: " + isr.mkString(","))
    partitionMetadataString.append("\tisUnderReplicated: %s".format(if(isr.size < replicas.size) "true" else "false"))
    partitionMetadataString.toString()
  }

}


