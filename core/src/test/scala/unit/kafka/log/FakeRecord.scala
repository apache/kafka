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

package unit.kafka.log

import java.lang.{Long => JLong}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.record.{Record, TimestampType}
import org.apache.kafka.common.utils.ByteUtils

class FakeRecord(fakeKey: ByteBuffer, fakeOffset: Number, fakeVersion: Long) extends Record {

  private var fakeHeaders: Array[Header] = _

  init()

  private def init(): Unit = {
    if (Option(fakeVersion).isEmpty)
      fakeHeaders = new Array[Header](0)
    else
      fakeHeaders = Array[Header](
        new RecordHeader("version", longToByte(fakeVersion))
      )
  }

  private def longToByte(value: Long): Array[Byte] = {
    var buffer = ByteBuffer.allocate(8)
    ByteUtils.writeVarlong(value, buffer)
    buffer.flip
    buffer.array
  }

  override def offset: Long = if (fakeOffset != null) fakeOffset.longValue else -1

  override def sequence = 0

  override def sizeInBytes = 0

  override def checksumOrNull: JLong = null

  override def timestamp: Long = -1

  override def isValid = true

  override def ensureValid(): Unit = {
  }

  override def keySize: Int = if (hasKey) fakeKey.remaining else -1

  override def hasKey: Boolean = fakeKey != null

  override def key: ByteBuffer = if (hasKey) fakeKey.duplicate() else null

  override def valueSize: Int = -1

  override def hasValue: Boolean = false

  override def value: ByteBuffer = null

  override def hasMagic(magic: Byte) = false

  override def isCompressed = false

  override def hasTimestampType(timestampType: TimestampType) = false

  override def headers: Array[Header] = fakeHeaders
}
