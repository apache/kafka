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

import scala.collection.mutable

/**
 * A bucket of consumers that are scheduled for heartbeat expiration.
 *
 * The motivation behind this is to avoid expensive fine-grained per-consumer
 * heartbeat expiration but use coarsen-grained methods that group consumers
 * with similar deadline together. This will result in some consumers not
 * being expired for heartbeats in time but is tolerable.
 */
class HeartbeatBucket(val startMs: Long, endMs: Long) {

  /* The list of consumers that are contained in this bucket */
  val consumerRegistryList = new mutable.HashSet[ConsumerRegistry]

  // TODO
}
