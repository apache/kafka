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

/**
 * Delayed join-group operations that are kept in the purgatory before the partition assignment completed
 *
 * These operation should never expire; when the rebalance has completed, all consumer's
 * join-group operations will be completed by sending back the response with the
 * calculated partition assignment.
 */
class DelayedJoinGroup(sessionTimeout: Long,
                       consumerRegistry: ConsumerRegistry,
                       responseCallback: () => Unit) extends DelayedOperation(sessionTimeout) {

  /* always successfully complete the operation once called */
  override def tryComplete(): Boolean = {
    forceComplete()
  }

  override def onExpiration() {
    // TODO
  }

  /* always assume the partition is already assigned as this delayed operation should never time-out */
  override def onComplete() {

    // TODO
    responseCallback
  }
}