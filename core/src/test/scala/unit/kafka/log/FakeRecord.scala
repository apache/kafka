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

package kafka.log

import java.lang.{Long => JLong}
import java.nio.ByteBuffer

import kafka.utils.TestUtils
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.record.{Record, TimestampType}

class FakeRecord(fakeKey: ByteBuffer, fakeOffset: Number, fakeSequence: Long = -1L, fakeTimestamp: Long = -1L) extends Record {
  private var fakeHeaders: Array[Header] = _

  init()

  private def init(): Unit = {
    if (Option(fakeSequence).isEmpty || fakeSequence <= 0)
      fakeHeaders = new Array[Header](0)
    else
      fakeHeaders = Array[Header](new RecordHeader("sequence", TestUtils.longToByte(fakeSequence)))
  }

  override def offset: Long = if (fakeOffset != null) fakeOffset.longValue else -1

  override def sequence = 0

  override def sizeInBytes = 0

  override def timestamp: Long = fakeTimestamp

  override def checksumOrNull: JLong = null

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
