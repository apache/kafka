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
package kafka.producer

/**
 * A partitioner controls the mapping between user-provided keys and kafka partitions. Users can implement a custom
 * partitioner to change this mapping.
 * 
 * Implementations will be constructed via reflection and are required to have a constructor that takes a single 
 * VerifiableProperties instance--this allows passing configuration properties into the partitioner implementation.
 */
@deprecated("This trait has been deprecated and will be removed in a future release. " +
            "Please use org.apache.kafka.clients.producer.Partitioner instead.", "0.10.0.0")
trait Partitioner {
  /**
   * Uses the key to calculate a partition bucket id for routing
   * the data to the appropriate broker partition
   * @return an integer between 0 and numPartitions-1
   */
  def partition(key: Any, numPartitions: Int): Int
}
