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
import kafka.server.ReplicaManager
import java.util.concurrent.atomic.AtomicLong

class Replica(val brokerId: Int,
              val partition: Partition,
              time: Time = SystemTime,
              initialHighWatermarkValue: Long = 0L,
              val log: Option[Log] = None) extends Logging {
  //only defined in local replica
  private[this] var highWatermarkValue: AtomicLong = new AtomicLong(initialHighWatermarkValue)
  // only used for remote replica; logEndOffsetValue for local replica is kept in log
  private[this] var logEndOffsetValue = new AtomicLong(ReplicaManager.UnknownLogEndOffset)
  private[this] var logEndOffsetUpdateTimeMsValue: AtomicLong = new AtomicLong(time.milliseconds)
  val topic = partition.topic
  val partitionId = partition.partitionId

  def logEndOffset_=(newLogEndOffset: Long) {
    if (!isLocal) {
      logEndOffsetValue.set(newLogEndOffset)
      logEndOffsetUpdateTimeMsValue.set(time.milliseconds)
      trace("Setting log end offset for replica %d for topic %s partition %d to %d"
            .format(brokerId, topic, partitionId, logEndOffsetValue.get()))
    } else
      throw new KafkaException("Shouldn't set logEndOffset for replica %d topic %s partition %d since it's local"
          .format(brokerId, topic, partitionId))

  }

  def logEndOffset = {
    if (isLocal)
      log.get.logEndOffset
    else
      logEndOffsetValue.get()
  }
  
  def isLocal: Boolean = {
    log match {
      case Some(l) => true
      case None => false
    }
  }

  def logEndOffsetUpdateTimeMs = logEndOffsetUpdateTimeMsValue.get()

  def highWatermark_=(newHighWatermark: Long) {
    if (isLocal) {
      trace("Setting hw for replica %d topic %s partition %d on broker %d to %d"
              .format(brokerId, topic, partitionId, brokerId, newHighWatermark))
      highWatermarkValue.set(newHighWatermark)
    } else
      throw new KafkaException("Unable to set highwatermark for replica %d topic %s partition %d since it's not local"
              .format(brokerId, topic, partitionId))
  }

  def highWatermark = {
    if (isLocal)
      highWatermarkValue.get()
    else
      throw new KafkaException("Unable to get highwatermark for replica %d topic %s partition %d since it's not local"
              .format(brokerId, topic, partitionId))
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
    replicaString.append("; Partition: " + partition.partitionId)
    replicaString.append("; isLocal: " + isLocal)
    if(isLocal) replicaString.append("; Highwatermark: " + highWatermark)
    replicaString.toString()
  }
}
