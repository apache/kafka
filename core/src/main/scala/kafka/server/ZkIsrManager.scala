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

import kafka.utils.{Logging, ReplicationUtils}
import kafka.zk.KafkaZkClient
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.Errors

class ZkIsrManager(zkClient: KafkaZkClient) extends AlterIsrManager with Logging {
  override def start(): Unit = {
    // No async processing is done, so nothing to do here
  }

  override def clearPending(topicPartition: TopicPartition): Unit = {
    // Since we always immediately process ZK updates and never actually enqueue anything, there is nothing to
    // clear here so this is a no-op
  }

  override def enqueue(alterIsrItem: AlterIsrItem): Boolean = {
    debug(s"Writing new ISR to ZooKeeper")

    val (updateSucceeded, newVersion) = ReplicationUtils.updateLeaderAndIsr(zkClient, alterIsrItem.topicPartition,
      alterIsrItem.leaderAndIsr, alterIsrItem.controllerEpoch)

    if (updateSucceeded) {
      // Return the given LeaderAndIsr with the new Zk version
      alterIsrItem.callback.apply(Right(alterIsrItem.leaderAndIsr.withZkVersion(newVersion)))
    } else {
      alterIsrItem.callback.apply(Left(Errors.INVALID_UPDATE_VERSION))
    }

    // Return true since we unconditionally accept the AlterIsrItem. The result of the operation is indicated by the
    // callback, not the return value of this method
    true
  }
}
