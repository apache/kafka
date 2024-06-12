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

import java.util.concurrent.ConcurrentLinkedQueue

import kafka.utils.Logging

/**
 * The action queue is used to collect actions which need to be executed later.
 */
trait ActionQueue {

  /**
   * add action to this queue.
   * @param action action
   */
  def add(action: () => Unit): Unit

  /**
   * try to complete all delayed actions
   */
  def tryCompleteActions(): Unit
}

/**
 * This queue is used to collect actions which need to be executed later. One use case is that ReplicaManager#appendRecords
 * produces record changes so we need to check and complete delayed requests. In order to avoid conflicting locking,
 * we add those actions to this queue and then complete them at the end of KafkaApis.handle() or DelayedJoin.onExpiration.
 */
class DelayedActionQueue extends Logging with ActionQueue {
  private val queue = new ConcurrentLinkedQueue[() => Unit]()

  def add(action: () => Unit): Unit = queue.add(action)

  def tryCompleteActions(): Unit = {
    val maxToComplete = queue.size()
    var count = 0
    var done = false
    while (!done && count < maxToComplete) {
      try {
        val action = queue.poll()
        if (action == null) done = true
        else action()
      } catch {
        case e: Throwable =>
          error("failed to complete delayed actions", e)
      } finally count += 1
    }
  }
}
