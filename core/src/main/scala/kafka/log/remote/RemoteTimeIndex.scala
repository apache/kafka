/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.log.remote

import java.io.File
import java.nio.ByteBuffer

import kafka.log.{AbstractIndex, CorruptIndexException, IndexEntry}

class RemoteTimeIndex(_file: File, baseOffset: Long, maxIndexSize: Int = -1, writable: Boolean = true)
  extends AbstractIndex[Long, Long](_file, baseOffset, maxIndexSize, writable) {

  override protected def entrySize: Int = ???

  /**
   * Do a basic sanity check on this index to detect obvious problems
   *
   * @throws CorruptIndexException if any problems are found
   */
  override def sanityCheck(): Unit = ???

  /**
   * Remove all the entries from the index.
   */
  override protected def truncate(): Unit = ???

  /**
   * Remove all entries from the index which have an offset greater than or equal to the given offset.
   * Truncating to an offset larger than the largest in the index has no effect.
   */
  override def truncateTo(offset: Long): Unit = ???

  /**
   * To parse an entry in the index.
   *
   * @param buffer the buffer of this memory mapped index.
   * @param n      the slot
   * @return the index entry stored in the given slot.
   */
  override protected def parseEntry(buffer: ByteBuffer, n: Int): IndexEntry = ???
}
