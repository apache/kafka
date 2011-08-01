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

package kafka.utils

import java.util.ArrayList
import java.util.concurrent._
import collection.JavaConversions

class Pool[K,V] extends Iterable[(K, V)] {

  private val pool = new ConcurrentHashMap[K, V]
  
  def this(m: collection.Map[K, V]) {
    this()
    for((k,v) <- m.elements)
      pool.put(k, v)
  }
  
  def put(k: K, v: V) = pool.put(k, v)
  
  def putIfNotExists(k: K, v: V) = pool.putIfAbsent(k, v)
  
  def contains(id: K) = pool.containsKey(id)
  
  def get(key: K): V = pool.get(key)
  
  def remove(key: K): V = pool.remove(key)
  
  def keys = JavaConversions.asSet(pool.keySet())
  
  def values: Iterable[V] = 
    JavaConversions.asIterable(new ArrayList[V](pool.values()))
  
  def clear: Unit = pool.clear()
  
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
