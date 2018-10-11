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

import java.util.concurrent.CountDownLatch
import java.util.concurrent.locks.ReentrantReadWriteLock

import kafka.utils.CoreUtils.{inReadLock, inWriteLock}
import kafka.utils.Logging
import org.apache.kafka.common.requests.AbstractControlRequest

// A help class to get/update the broker epoch cached in the current broker
class BrokerEpoch(brokerId: Int) extends Logging {
  var curBrokerEpoch: Long = AbstractControlRequest.UNKNOWN_BROKER_EPOCH
  val readWriteLock = new ReentrantReadWriteLock
  val initializeLatch = new CountDownLatch(1)

  def get: Long = {
    // We need to block reads for broker epoch until it has been initialized (after finishing broker znode registration in zk).
    // Otherwise, the broker may get back UNKNOWN_BROKER_EPOCH and accept requests with stale broker epochs after bouncing.
    initializeLatch.await()
    inReadLock(readWriteLock) {
      curBrokerEpoch
    }
  }

  def update(newEpoch: Long): Unit = {
    inWriteLock(readWriteLock) {
      if (newEpoch > curBrokerEpoch) {
        info(s"Update broker epoch for $brokerId from $curBrokerEpoch to $newEpoch")
        curBrokerEpoch = newEpoch
      } else {
        info(s"Skip broker epoch update for $brokerId because the $newEpoch is not larger than cached broker epoch:$curBrokerEpoch")
      }
    }
    initializeLatch.countDown()
  }
}
