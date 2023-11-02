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

import java.util.concurrent._

import org.apache.kafka.common.KafkaException

import collection.Set
import scala.jdk.CollectionConverters._

class Pool[K,V](valueFactory: Option[K => V] = None) extends Iterable[(K, V)] {

  private val pool: ConcurrentMap[K, V] = new ConcurrentHashMap[K, V]

  def put(k: K, v: V): V = pool.put(k, v)

  def putAll(map: java.util.Map[K, V]): Unit = pool.putAll(map)

  def putIfNotExists(k: K, v: V): V = pool.putIfAbsent(k, v)

  /**
   * Gets the value associated with the given key. If there is no associated
   * value, then create the value using the pool's value factory and return the
   * value associated with the key. The user should declare the factory method
   * as lazy if its side-effects need to be avoided.
   *
   * @param key The key to lookup.
   * @return The final value associated with the key.
   */
  def getAndMaybePut(key: K): V = {
    if (valueFactory.isEmpty)
      throw new KafkaException("Empty value factory in pool.")
    getAndMaybePut(key, valueFactory.get(key))
  }

  /**
    * Gets the value associated with the given key. If there is no associated
    * value, then create the value using the provided by `createValue` and return the
    * value associated with the key.
    *
    * @param key The key to lookup.
    * @param createValue Factory function.
    * @return The final value associated with the key.
    */
  def getAndMaybePut(key: K, createValue: => V): V =
    pool.computeIfAbsent(key, _ => createValue)

  def contains(id: K): Boolean = pool.containsKey(id)
  
  def get(key: K): V = pool.get(key)
  
  def remove(key: K): V = pool.remove(key)

  def remove(key: K, value: V): Boolean = pool.remove(key, value)

  def removeAll(keys: Iterable[K]): Unit = pool.keySet.removeAll(keys.asJavaCollection)

  def keys: Set[K] = pool.keySet.asScala

  def values: Iterable[V] = pool.values.asScala

  def clear(): Unit = { pool.clear() }

  def foreachEntry(f: (K, V) => Unit): Unit = {
    pool.forEach((k, v) => f(k, v))
  }

  override def size: Int = pool.size
  
  override def iterator: Iterator[(K, V)] = new Iterator[(K,V)]() {
    
    private val iter = pool.entrySet.iterator
    
    def hasNext: Boolean = iter.hasNext
    
    def next(): (K, V) = {
      val n = iter.next
      (n.getKey, n.getValue)
    }
    
  }
    
}
