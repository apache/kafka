/*
 * Copyright 2010 LinkedIn
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.consumer.storage

import java.util.concurrent._
import java.util.concurrent.atomic._
import java.util.concurrent.locks._

class MemoryOffsetStorage extends OffsetStorage {
  
  val offsetAndLock = new ConcurrentHashMap[(Int, String), (AtomicLong, Lock)]

  def reserve(node: Int, topic: String): Long = {
    val key = (node, topic)
    if(!offsetAndLock.containsKey(key))
      offsetAndLock.putIfAbsent(key, (new AtomicLong(0), new ReentrantLock))
    val (offset, lock) = offsetAndLock.get(key)
    lock.lock
    offset.get
  }

  def commit(node: Int, topic: String, offset: Long) = {
    val (highwater, lock) = offsetAndLock.get((node, topic))
    highwater.set(offset)
    lock.unlock
    offset
  }
  
}
