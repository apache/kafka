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

import kafka.server.DelayedOperation
import java.util.concurrent.atomic.AtomicBoolean


/**
 * Delayed rebalance operations that are added to the purgatory when group is preparing for rebalance
 *
 * Whenever a join-group request is received, check if all known consumers have requested
 * to re-join the group; if yes, complete this operation to proceed rebalance.
 *
 * When the operation has expired, any known consumers that have not requested to re-join
 * the group are marked as failed, and complete this operation to proceed rebalance with
 * the rest of the group.
 */
class DelayedRebalance(sessionTimeout: Long,
                       groupRegistry: GroupRegistry,
                       rebalanceCallback: String => Unit,
                       failureCallback: (String, String) => Unit)
  extends DelayedOperation(sessionTimeout) {

  val allConsumersJoinedGroup = new AtomicBoolean(false)

  /* check if all known consumers have requested to re-join group */
  override def tryComplete(): Boolean = {
    allConsumersJoinedGroup.set(groupRegistry.memberRegistries.values.foldLeft
      (true) ((agg, cur) => agg && cur.joinGroupReceived.get()))

    if (allConsumersJoinedGroup.get())
      forceComplete()
    else
      false
  }

  override def onExpiration() {
    // TODO
  }

  /* mark consumers that have not re-joined group as failed and proceed to rebalance the rest of the group */
  override def onComplete() {
    groupRegistry.memberRegistries.values.foreach(consumerRegistry =>
      if (!consumerRegistry.joinGroupReceived.get())
        failureCallback(groupRegistry.groupId, consumerRegistry.consumerId)
    )

    rebalanceCallback(groupRegistry.groupId)
  }
}
