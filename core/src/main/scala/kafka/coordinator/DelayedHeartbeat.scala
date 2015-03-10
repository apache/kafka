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
 * Delayed heartbeat operations that are added to the purgatory for session-timeout checking
 *
 * These operations will always be expired. Once it has expired, all its
 * currently contained consumers are marked as heartbeat timed out.
 */
class DelayedHeartbeat(sessionTimeout: Long,
                       bucket: HeartbeatBucket,
                       expireCallback: (String, String) => Unit)
  extends DelayedOperation(sessionTimeout) {

  /* this function should never be called */
  override def tryComplete(): Boolean = {

    throw new IllegalStateException("Delayed heartbeat purgatory should never try to complete any bucket")
  }

  override def onExpiration() {
    // TODO
  }

  /* mark all consumers within the heartbeat as heartbeat timed out */
  override def onComplete() {
    for (registry <- bucket.consumerRegistryList)
      expireCallback(registry.groupId, registry.consumerId)
  }
}
