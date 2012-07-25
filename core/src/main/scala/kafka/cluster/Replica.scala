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

package kafka.cluster

import kafka.log.Log
import kafka.utils.{SystemTime, Time, Logging}
import kafka.common.KafkaException

class Replica(val brokerId: Int,
              val partition: Partition,
              val topic: String,
              time: Time = SystemTime,
              var hw: Option[Long] = None,
              var log: Option[Log] = None) extends Logging {
  private var logEndOffset: Long = -1L
  private var logEndOffsetUpdateTimeMs: Long = -1L

  def logEndOffset(newLeo: Option[Long] = None): Long = {
    isLocal match {
      case true =>
        newLeo match {
          case Some(newOffset) => logEndOffsetUpdateTimeMs = time.milliseconds; newOffset
          case None => log.get.logEndOffset
        }
      case false =>
        newLeo match {
          case Some(newOffset) =>
            logEndOffset = newOffset
            logEndOffsetUpdateTimeMs = time.milliseconds
            trace("Setting log end offset for replica %d for topic %s partition %d to %d"
              .format(brokerId, topic, partition.partitionId, logEndOffset))
            logEndOffset
          case None => logEndOffset
        }
    }
  }

  def isLocal: Boolean = {
    log match {
      case Some(l) => true
      case None => false
    }
  }

  def logEndOffsetUpdateTime = logEndOffsetUpdateTimeMs

  def highWatermark(highwaterMarkOpt: Option[Long] = None): Long = {
    highwaterMarkOpt match {
      case Some(highwaterMark) =>
        isLocal match {
          case true =>
            trace("Setting hw for topic %s partition %d on broker %d to %d".format(topic, partition.partitionId,
                                                                                   brokerId, highwaterMark))
            hw = Some(highwaterMark)
            highwaterMark
          case false => throw new KafkaException("Unable to set highwatermark for topic %s ".format(topic) +
            "partition %d on broker %d, since there is no local log for this partition"
              .format(partition.partitionId, brokerId))
        }
      case None =>
        isLocal match {
          case true =>
            hw match {
              case Some(highWatermarkValue) => highWatermarkValue
              case None => throw new KafkaException("HighWatermark does not exist for topic %s ".format(topic) +
              " partition %d on broker %d but local log exists".format(partition.partitionId, brokerId))
            }
          case false => throw new KafkaException("Unable to get highwatermark for topic %s ".format(topic) +
            "partition %d on broker %d, since there is no local log for this partition"
              .format(partition.partitionId, brokerId))
        }
    }
  }

  override def equals(that: Any): Boolean = {
    if(!(that.isInstanceOf[Replica]))
      return false
    val other = that.asInstanceOf[Replica]
    if(topic.equals(other.topic) && brokerId == other.brokerId && partition.equals(other.partition))
      return true
    false
  }

  override def hashCode(): Int = {
    31 + topic.hashCode() + 17*brokerId + partition.hashCode()
  }


  override def toString(): String = {
    val replicaString = new StringBuilder
    replicaString.append("ReplicaId: " + brokerId)
    replicaString.append("; Topic: " + topic)
    replicaString.append("; Partition: " + partition.toString)
    replicaString.append("; isLocal: " + isLocal)
    if(isLocal) replicaString.append("; Highwatermark: " + highWatermark())
    replicaString.toString()
  }
}
