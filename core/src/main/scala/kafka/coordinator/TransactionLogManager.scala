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

package kafka.coordinator

import kafka.common.Topic
import kafka.utils.{Logging, ZkUtils}
import org.apache.kafka.common.utils.Utils

import scala.collection.mutable
import java.util.concurrent.locks.ReentrantLock

import kafka.utils.CoreUtils.inLock


/**
  * Transaction log manager is part of the transaction coordinator that manages the transaction log, which is
  * a special internal topic.
  */
class TransactionLogManager(val brokerId: Int,
                            val zkUtils: ZkUtils) extends Logging {

  this.logIdent = "[Transaction Log Manager " + brokerId + "]: "

  /* number of partitions for the transaction log topic */
  private val transactionTopicPartitionCount = getTransactionTopicPartitionCount

  /* lock protecting access to loading and owned partition sets */
  private val partitionLock = new ReentrantLock()

  /* partitions of transaction topics that are assigned to this manager */
  private val ownedPartitions: mutable.Set[Int] = mutable.Set()

  def partitionFor(transactionalId: String): Int = Utils.abs(transactionalId.hashCode) % transactionTopicPartitionCount

  def isCoordinatorFor(transactionalId: String): Boolean = inLock(partitionLock) { ownedPartitions.contains(partitionFor(transactionalId)) }

  /**
    * Gets the partition count of the transaction log topic from ZooKeeper.
    * If the topic does not exist, the default partition count is returned.
    */
  private def getTransactionTopicPartitionCount: Int = {
    zkUtils.getTopicPartitionCount(Topic.TransactionStateTopicName).getOrElse(50)  // TODO: need a config for this
  }

  /**
    * Add the partition into the owned list
    *
    * TODO: this is for test only and should be augmented with txn log bootstrapping
    */
  def addPartitionOwnership(partition: Int) {
    inLock(partitionLock) {
      ownedPartitions.add(partition)
    }
  }

  /**
    * Remove the partition from the owned list
    *
    * TODO: this is for test only and should be augmented with cache cleaning
    */
  def removePartitionOwnership(offsetsPartition: Int): Unit = {
    inLock(partitionLock) {
      ownedPartitions.remove(offsetsPartition)
    }
  }
}
