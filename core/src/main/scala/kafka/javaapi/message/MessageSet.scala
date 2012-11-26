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

package kafka.javaapi.message


import kafka.message.{MessageAndOffset, InvalidMessageException}


/**
 * A set of messages. A message set has a fixed serialized form, though the container
 * for the bytes could be either in-memory or on disk. A The format of each message is
 * as follows:
 * 4 byte size containing an integer N
 * N message bytes as described in the message class
 */
abstract class MessageSet extends java.lang.Iterable[MessageAndOffset] {

  /**
   * Provides an iterator over the messages in this set
   */
  def iterator: java.util.Iterator[MessageAndOffset]

  /**
   * Gives the total size of this message set in bytes
   */
  def sizeInBytes: Int

  /**
   * Validate the checksum of all the messages in the set. Throws an InvalidMessageException if the checksum doesn't
   * match the payload for any message.
   */
  def validate(): Unit = {
    val thisIterator = this.iterator
    while(thisIterator.hasNext) {
      val messageAndOffset = thisIterator.next
      if(!messageAndOffset.message.isValid)
        throw new InvalidMessageException
    }
  }
}
