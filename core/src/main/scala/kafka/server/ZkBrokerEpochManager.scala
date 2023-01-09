/*
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

import kafka.controller.KafkaController
import org.apache.kafka.common.requests.AbstractControlRequest

class ZkBrokerEpochManager(metadataCache: MetadataCache,
                           controller: KafkaController,
                           lifecycleManagerOpt: Option[BrokerLifecycleManager]) {
  def get(): Long = {
    lifecycleManagerOpt match {
      case Some(lifecycleManager) => metadataCache.getControllerId match {
        case Some(_: ZkCachedControllerId) => controller.brokerEpoch
        case Some(_: KRaftCachedControllerId) => lifecycleManager.brokerEpoch
        case None => controller.brokerEpoch
      }
      case None => controller.brokerEpoch
    }
  }

  def isBrokerEpochStale(brokerEpochInRequest: Long, isKRaftControllerRequest: Boolean): Boolean = {
    if (brokerEpochInRequest == AbstractControlRequest.UNKNOWN_BROKER_EPOCH) {
      false
    } else if (isKRaftControllerRequest) {
      if (lifecycleManagerOpt.isDefined) {
        brokerEpochInRequest < lifecycleManagerOpt.get.brokerEpoch
      } else {
        throw new IllegalStateException("Expected BrokerLifecycleManager to be non-null.")
      }
    } else {
      // brokerEpochInRequest > controller.brokerEpoch is possible in rare scenarios where the controller gets notified
      // about the new broker epoch and sends a control request with this epoch before the broker learns about it
      brokerEpochInRequest < controller.brokerEpoch
    }
  }
}
