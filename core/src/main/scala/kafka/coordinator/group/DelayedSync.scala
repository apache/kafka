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

package kafka.coordinator.group

/**
 * Delayed rebalance operation that is added to the purgatory when the group is completing the
 * rebalance.
 *
 * Whenever a SyncGroup is received, checks that we received all the SyncGroup request from
 * each member of the group; if yes, complete this operation.
 *
 * When the operation has expired, any known members that have not sent a SyncGroup requests
 * are removed from the group. If any members is removed, the group is rebalanced.
 */
private[group] class DelayedSync(
  coordinator: GroupCoordinator,
  group: GroupMetadata,
  generationId: Int,
  rebalanceTimeoutMs: Long
) extends DelayedRebalance(
  rebalanceTimeoutMs,
  group.lock
) {
  override def tryComplete(): Boolean = {
    coordinator.tryCompletePendingSync(group, generationId, forceComplete _)
  }

  override def onExpiration(): Unit = {
    coordinator.onExpirePendingSync(group, generationId)
  }

  override def onComplete(): Unit = { }
}
