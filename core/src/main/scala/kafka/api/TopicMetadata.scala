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
import kafka.common._
import org.apache.kafka.common.utils.Utils._

object TopicMetadata {
  
  val NoLeaderNodeId = -1

  def readFrom(buffer: ByteBuffer, brokers: Map[Int, Broker]): TopicMetadata = {
    val errorCode = readShortInRange(buffer, "error code", (-1, Short.MaxValue))
    val topic = readShortString(buffer)
    val numPartitions = readIntInRange(buffer, "number of partitions", (0, Int.MaxValue))
    val partitionsMetadata: Array[PartitionMetadata] = new Array[PartitionMetadata](numPartitions)
    for(i <- 0 until numPartitions) {
      val partitionMetadata = PartitionMetadata.readFrom(buffer, brokers)
      partitionsMetadata(partitionMetadata.partitionId) = partitionMetadata
    }
    new TopicMetadata(topic, partitionsMetadata, errorCode)
  }
}

case class TopicMetadata(topic: String, partitionsMetadata: Seq[PartitionMetadata], errorCode: Short = ErrorMapping.NoError) extends Logging {
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

  override def toString(): String = {
    val topicMetadataInfo = new StringBuilder
    topicMetadataInfo.append("{TopicMetadata for topic %s -> ".format(topic))
    errorCode match {
      case ErrorMapping.NoError =>
        partitionsMetadata.foreach { partitionMetadata =>
          partitionMetadata.errorCode match {
            case ErrorMapping.NoError =>
              topicMetadataInfo.append("\nMetadata for partition [%s,%d] is %s".format(topic,
                partitionMetadata.partitionId, partitionMetadata.toString()))
            case ErrorMapping.ReplicaNotAvailableCode =>
              // this error message means some replica other than the leader is not available. The consumer
              // doesn't care about non leader replicas, so ignore this
              topicMetadataInfo.append("\nMetadata for partition [%s,%d] is %s".format(topic,
                partitionMetadata.partitionId, partitionMetadata.toString()))
            case _ =>
              topicMetadataInfo.append("\nMetadata for partition [%s,%d] is not available due to %s".format(topic,
                partitionMetadata.partitionId, ErrorMapping.exceptionFor(partitionMetadata.errorCode).getClass.getName))
          }
        }
      case _ =>
        topicMetadataInfo.append("\nNo partition metadata for topic %s due to %s".format(topic,
                                 ErrorMapping.exceptionFor(errorCode).getClass.getName))
    }
    topicMetadataInfo.append("}")
    topicMetadataInfo.toString()
  }
}

object PartitionMetadata {

  def readFrom(buffer: ByteBuffer, brokers: Map[Int, Broker]): PartitionMetadata = {
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
                             val leader: Option[Broker], 
                             replicas: Seq[Broker], 
                             isr: Seq[Broker] = Seq.empty,
                             errorCode: Short = ErrorMapping.NoError) extends Logging {
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

  override def toString(): String = {
    val partitionMetadataString = new StringBuilder
    partitionMetadataString.append("\tpartition " + partitionId)
    partitionMetadataString.append("\tleader: " + (if(leader.isDefined) formatBroker(leader.get) else "none"))
    partitionMetadataString.append("\treplicas: " + replicas.map(formatBroker).mkString(","))
    partitionMetadataString.append("\tisr: " + isr.map(formatBroker).mkString(","))
    partitionMetadataString.append("\tisUnderReplicated: %s".format(if(isr.size < replicas.size) "true" else "false"))
    partitionMetadataString.toString()
  }

  private def formatBroker(broker: Broker) = broker.id + " (" + formatAddress(broker.host, broker.port) + ")"
}


