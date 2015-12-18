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

package kafka.utils

import java.util.ArrayList
import java.util.concurrent._
import collection.mutable
import collection.JavaConversions
import kafka.common.KafkaException

class Pool[K,V](valueFactory: Option[(K) => V] = None) extends Iterable[(K, V)] {

  private val pool: ConcurrentMap[K, V] = new ConcurrentHashMap[K, V]
  private val createLock = new Object

  def this(m: collection.Map[K, V]) {
    this()
    m.foreach(kv => pool.put(kv._1, kv._2))
  }
  
  def put(k: K, v: V) = pool.put(k, v)
  
  def putIfNotExists(k: K, v: V) = pool.putIfAbsent(k, v)

  /**
   * Gets the value associated with the given key. If there is no associated
   * value, then create the value using the pool's value factory and return the
   * value associated with the key. The user should declare the factory method
   * as lazy if its side-effects need to be avoided.
   *
   * @param key The key to lookup.
   * @return The final value associated with the key. This may be different from
   *         the value created by the factory if another thread successfully
   *         put a value.
   */
  def getAndMaybePut(key: K) = {
    if (valueFactory.isEmpty)
      throw new KafkaException("Empty value factory in pool.")
    val curr = pool.get(key)
    if (curr == null) {
      createLock synchronized {
        val curr = pool.get(key)
        if (curr == null)
          pool.put(key, valueFactory.get(key))
        pool.get(key)
      }
    }
    else
      curr
  }

  def contains(id: K) = pool.containsKey(id)
  
  def get(key: K): V = pool.get(key)
  
  def remove(key: K): V = pool.remove(key)

  def remove(key: K, value: V): Boolean = pool.remove(key, value)

  def keys: mutable.Set[K] = {
    import JavaConversions._
    pool.keySet()
  }
  
  def values: Iterable[V] = {
    import JavaConversions._
    new ArrayList[V](pool.values())
  }
  
  def clear() { pool.clear() }
  
  override def size = pool.size
  
  override def iterator = new Iterator[(K,V)]() {
    
    private val iter = pool.entrySet.iterator
    
    def hasNext: Boolean = iter.hasNext
    
    def next: (K, V) = {
      val n = iter.next
      (n.getKey, n.getValue)
    }
    
  }
    
}
