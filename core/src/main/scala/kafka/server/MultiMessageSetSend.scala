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

import kafka.network._
import kafka.utils._

/**
 * A set of message sets prefixed by size
 */
@nonthreadsafe
private[server] class MultiMessageSetSend(val sets: List[MessageSetSend]) extends MultiSend(new ByteBufferSend(6) :: sets) {
  
  val buffer = this.sends.head.asInstanceOf[ByteBufferSend].buffer
  val allMessageSetSize: Int = sets.foldLeft(0)(_ + _.sendSize)
  val expectedBytesToWrite: Int = 4 + 2 + allMessageSetSize
  buffer.putInt(2 + allMessageSetSize)
  buffer.putShort(0)
  buffer.rewind()
  
}
