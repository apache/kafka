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

/**
 * @param checkIntervalMs How often to check for ISR
 * @param maxDelayMs  Maximum time that an ISR change may be delayed before sending the notification
 * @param lingerMs  Maximum time to await additional changes before sending the notification
 */
case class IsrChangePropagationConfig(checkIntervalMs: Long, maxDelayMs: Long, lingerMs: Long)

object ZkIsrManager {
  // This field is mutable to allow overriding change notification behavior in test cases
  @volatile var DefaultIsrPropagationConfig: IsrChangePropagationConfig = IsrChangePropagationConfig(
    checkIntervalMs = 2500,
    lingerMs = 5000,
    maxDelayMs = 60000,
  )
}

class ZkIsrManager(scheduler: Scheduler, time: Time, zkClient: KafkaZkClient) extends AlterIsrManager with Logging {

  private val isrChangeNotificationConfig = ZkIsrManager.DefaultIsrPropagationConfig
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

  override def submit(alterIsrItem: AlterIsrItem): Boolean = {
    debug(s"Writing new ISR ${alterIsrItem.leaderAndIsr.isr} to ZooKeeper with version " +
      s"${alterIsrItem.leaderAndIsr.zkVersion} for partition ${alterIsrItem.topicPartition}")

    val (updateSucceeded, newVersion) = ReplicationUtils.updateLeaderAndIsr(zkClient, alterIsrItem.topicPartition,
      alterIsrItem.leaderAndIsr, alterIsrItem.controllerEpoch)

    if (updateSucceeded) {
      // Track which partitions need to be propagated to the controller
      isrChangeSet synchronized {
        isrChangeSet += alterIsrItem.topicPartition
        lastIsrChangeMs.set(time.milliseconds())
      }

      // We rely on Partition#isrState being properly set to the pending ISR at this point since we are synchronously
      // applying the callback
      alterIsrItem.callback.apply(Right(alterIsrItem.leaderAndIsr.withZkVersion(newVersion)))
    } else {
      alterIsrItem.callback.apply(Left(Errors.INVALID_UPDATE_VERSION))
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
