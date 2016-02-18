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

package kafka.message

import org.apache.kafka.common.errors.CorruptRecordException

/**
 * Indicates that a message failed its checksum and is corrupt
 *
 * InvalidMessageException extends CorruptRecordException for temporary compatibility with the old Scala clients.
 * We want to update the server side code to use and catch the new CorruptRecordException.
 * Because ByteBufferMessageSet.scala and Message.scala are used in both server and client code having
 * InvalidMessageException extend CorruptRecordException allows us to change server code without affecting the client.
 */
class InvalidMessageException(message: String) extends CorruptRecordException(message) {
  def this() = this(null)
}
