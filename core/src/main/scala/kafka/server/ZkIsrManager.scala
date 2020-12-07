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

import kafka.utils.{Logging, ReplicationUtils, Scheduler}
import kafka.zk.KafkaZkClient
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.utils.Time

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable

class ZkIsrManager(scheduler: Scheduler, time: Time, zkClient: KafkaZkClient) extends AlterIsrManager with Logging {

  private val isrChangeNotificationConfig = ReplicaManager.DefaultIsrPropagationConfig
  // Visible for testing
  private[server] val isrChangeSet: mutable.Set[TopicPartition] = new mutable.HashSet[TopicPartition]()
  private val lastIsrChangeMs = new AtomicLong(time.milliseconds())
  private val lastIsrPropagationMs = new AtomicLong(time.milliseconds())

  override def start(): Unit = {
    scheduler.schedule("isr-change-propagation", maybePropagateIsrChanges _,
      period = isrChangeNotificationConfig.checkIntervalMs, unit = TimeUnit.MILLISECONDS)
  }

  override def clearPending(topicPartition: TopicPartition): Unit = {
    // Since we always immediately process ZK updates and never actually enqueue anything, there is nothing to
    // clear here so this is a no-op. Even if there are changes that have not been propagated, the write to ZK
    // has already happened, so we may as well send the notification to the controller.
  }

  override def enqueue(alterIsrItem: AlterIsrItem): Boolean = {
    debug(s"Writing new ISR " + alterIsrItem.leaderAndIsr.isr + " to ZooKeeper with version " +
      alterIsrItem.leaderAndIsr.zkVersion + " for partition " + alterIsrItem.topicPartition)

    val (updateSucceeded, newVersion) = ReplicationUtils.updateLeaderAndIsr(zkClient, alterIsrItem.topicPartition,
      alterIsrItem.leaderAndIsr, alterIsrItem.controllerEpoch)

    if (updateSucceeded) {
      // Track which partitions need to be propagated to the controller
      isrChangeSet synchronized {
        isrChangeSet += alterIsrItem.topicPartition
        lastIsrChangeMs.set(time.milliseconds())
      }

      // We actually need to apply the callback in another thread since Partition#sendAlterIsrRequest will write
      // isrState after enqueuing the AlterIsrItem. This is only safe because the callback
      // (Partition#handleAlterIsrResponse) takes the ISR write lock.
      //
      // For the callback value, return the given LeaderAndIsr but updated with the new ZK version
      scheduler.schedule(
        "zk-async-callback",
        () => alterIsrItem.callback.apply(Right(alterIsrItem.leaderAndIsr.withZkVersion(newVersion))))
    } else {
      scheduler.schedule(
        "zk-async-callback",
        () => alterIsrItem.callback.apply(Left(Errors.INVALID_UPDATE_VERSION)))
    }

    // Return true since we unconditionally accept the AlterIsrItem. The result of the operation is indicated by the
    // callback, not the return value of this method
    true
  }

  /**
   * This function periodically runs to see if ISR needs to be propagated. It propagates ISR when:
   * 1. There is ISR change not propagated yet.
   * 2. There is no ISR Change in the last five seconds, or it has been more than 60 seconds since the last ISR propagation.
   * This allows an occasional ISR change to be propagated within a few seconds, and avoids overwhelming controller and
   * other brokers when large amount of ISR change occurs.
   */
  private[server] def maybePropagateIsrChanges(): Unit = {
    val now = time.milliseconds()
    isrChangeSet synchronized {
      if (isrChangeSet.nonEmpty &&
        (lastIsrChangeMs.get() + isrChangeNotificationConfig.lingerMs < now ||
          lastIsrPropagationMs.get() + isrChangeNotificationConfig.maxDelayMs < now)) {
        zkClient.propagateIsrChanges(isrChangeSet)
        isrChangeSet.clear()
        lastIsrPropagationMs.set(now)
      }
    }
  }
}
