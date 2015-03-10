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

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.HashMap

/**
 * Consumer registry metadata contains the following metadata:
 *
 *  Heartbeat metadata:
 *  1. negotiated heartbeat session timeout.
 *  2. recorded number of timed-out heartbeats.
 *  3. associated heartbeat bucket in the purgatory.
 *
 *  Subscription metadata:
 *  1. subscribed topic list
 *  2. assigned partitions for the subscribed topics.
 */
class ConsumerRegistry(val groupId: String,
                       val consumerId: String,
                       val topics: List[String],
                       val sessionTimeoutMs: Int) {

  /* number of expired heartbeat recorded */
  val numExpiredHeartbeat = new AtomicInteger(0)

  /* flag indicating if join group request is received */
  val joinGroupReceived = new AtomicBoolean(false)

  /* assigned partitions per subscribed topic */
  val assignedPartitions = new HashMap[String, List[Int]]

  /* associated heartbeat bucket */
  var currentHeartbeatBucket = null

}
