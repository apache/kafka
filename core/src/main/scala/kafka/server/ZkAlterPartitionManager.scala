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

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.CompletableFuture
import org.apache.kafka.common.TopicIdPartition
import org.apache.kafka.common.errors.InvalidUpdateVersionException
import org.apache.kafka.common.utils.Time
import org.apache.kafka.metadata.LeaderAndIsr
import org.apache.kafka.server.util.Scheduler

import scala.collection.mutable

/**
 * @param checkIntervalMs How often to check for ISR
 * @param maxDelayMs  Maximum time that an ISR change may be delayed before sending the notification
 * @param lingerMs  Maximum time to await additional changes before sending the notification
 */
case class IsrChangePropagationConfig(checkIntervalMs: Long, maxDelayMs: Long, lingerMs: Long)

object ZkAlterPartitionManager {
  // This field is mutable to allow overriding change notification behavior in test cases
  @volatile var DefaultIsrPropagationConfig: IsrChangePropagationConfig = IsrChangePropagationConfig(
    checkIntervalMs = 2500,
    lingerMs = 5000,
    maxDelayMs = 60000,
  )
}

class ZkAlterPartitionManager(scheduler: Scheduler, time: Time, zkClient: KafkaZkClient) extends AlterPartitionManager with Logging {

  private val isrChangeNotificationConfig = ZkAlterPartitionManager.DefaultIsrPropagationConfig
  // Visible for testing
  private[server] val isrChangeSet: mutable.Set[TopicPartition] = new mutable.HashSet[TopicPartition]()
  private val lastIsrChangeMs = new AtomicLong(time.milliseconds())
  private val lastIsrPropagationMs = new AtomicLong(time.milliseconds())

  override def start(): Unit = {
    scheduler.schedule("isr-change-propagation", () => maybePropagateIsrChanges(), 0L,
      isrChangeNotificationConfig.checkIntervalMs)
  }

  override def submit(
    topicIdPartition: TopicIdPartition,
    leaderAndIsr: LeaderAndIsr,
    controllerEpoch: Int
  ): CompletableFuture[LeaderAndIsr]= {
    debug(s"Writing new ISR ${leaderAndIsr.isr} to ZooKeeper with version " +
      s"${leaderAndIsr.partitionEpoch} for partition $topicIdPartition")

    val (updateSucceeded, newVersion) = ReplicationUtils.updateLeaderAndIsr(zkClient, topicIdPartition.topicPartition,
      leaderAndIsr, controllerEpoch)

    val future = new CompletableFuture[LeaderAndIsr]()
    if (updateSucceeded) {
      // Track which partitions need to be propagated to the controller
      isrChangeSet synchronized {
        isrChangeSet += topicIdPartition.topicPartition
        lastIsrChangeMs.set(time.milliseconds())
      }

      // We rely on Partition#isrState being properly set to the pending ISR at this point since we are synchronously
      // applying the callback
      future.complete(leaderAndIsr.withPartitionEpoch(newVersion))
    } else {
      future.completeExceptionally(new InvalidUpdateVersionException(
        s"ISR update $leaderAndIsr for partition $topicIdPartition with controller epoch $controllerEpoch " +
          "failed with an invalid version error"))
    }
    future
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
