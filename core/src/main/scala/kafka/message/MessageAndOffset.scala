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

import org.apache.kafka.common.record.{AbstractLegacyRecordBatch, Record, RecordBatch}

object MessageAndOffset {
  def fromRecordBatch(batch: RecordBatch): MessageAndOffset = {
    batch match {
      case legacyBatch: AbstractLegacyRecordBatch =>
        MessageAndOffset(Message.fromRecord(legacyBatch.outerRecord), legacyBatch.lastOffset)

      case _ =>
        throw new IllegalArgumentException(s"Illegal batch type ${batch.getClass}. The older message format classes " +
          s"only support conversion from ${classOf[AbstractLegacyRecordBatch]}, which is used for magic v0 and v1")
    }
  }

  def fromRecord(record: Record): MessageAndOffset = {
    record match {
      case legacyBatch: AbstractLegacyRecordBatch =>
        MessageAndOffset(Message.fromRecord(legacyBatch.outerRecord), legacyBatch.lastOffset)

      case _ =>
        throw new IllegalArgumentException(s"Illegal record type ${record.getClass}. The older message format classes " +
          s"only support conversion from ${classOf[AbstractLegacyRecordBatch]}, which is used for magic v0 and v1")
    }
  }
}

case class MessageAndOffset(message: Message, offset: Long) {
  
  /**
   * Compute the offset of the next message in the log
   */
  def nextOffset: Long = offset + 1

}

