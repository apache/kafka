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

import kafka.utils.Range

/**
 * A method for storing offsets for the consumer. 
 * This is used to track the progress of the consumer in the stream.
 */
trait OffsetStorage {

  /**
   * Reserve a range of the length given by increment.
   * @param increment The size to reserver
   * @return The range reserved
   */
  def reserve(node: Int, topic: String): Long

  /**
   * Update the offset to the new offset
   * @param offset The new offset
   */
  def commit(node: Int, topic: String, offset: Long)
  
}
