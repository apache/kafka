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
package kafka.server

import kafka.log.Log
import kafka.cluster.{Partition, Replica}
import kafka.utils.Logging
import collection.mutable

class ReplicaManager(config: KafkaConfig) extends Logging {

  private val replicas = new mutable.HashMap[(String, Int), Replica]()

  def addLocalReplica(topic: String, partitionId: Int, log: Log): Replica = {
    val replica = replicas.get((topic, partitionId))
    replica match {
      case Some(r) =>
        r.log match {
          case None =>
            r.log = Some(log)
          case Some(l) => // nothing to do since log already exists
        }
      case None =>
        val partition = new Partition(topic, partitionId)
        val replica = new Replica(config.brokerId, partition, topic, Some(log), log.getHighwaterMark, log.maxSize, true)
        replicas.put((topic, partitionId), replica)
        info("Added local replica for topic %s partition %s on broker %d"
          .format(replica.topic, replica.partition.partId, replica.brokerId))
    }
    replicas.get((topic, partitionId)).get
  }

  def addRemoteReplica(topic: String, partitionId: Int): Replica = {
    val replica = replicas.get((topic, partitionId))
    replica match {
      case Some(r) =>
      case None =>
        val partition = new Partition(topic, partitionId)
        val replica = new Replica(config.brokerId, partition, topic, None, -1, -1, false)
        replicas.put((topic, partitionId), replica)
        info("Added remote replica for topic %s partition %s on broker %d"
          .format(replica.topic, replica.partition.partId, replica.brokerId))
    }
    replicas.get((topic, partitionId)).get
  }

  def getReplica(topic: String, partitionId: Int): Option[Replica] = {
    replicas.get((topic, partitionId))
  }
}