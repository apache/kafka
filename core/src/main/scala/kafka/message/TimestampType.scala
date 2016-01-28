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

/**
 * The timestamp type of the messages.
 */
case object TimestampType extends Enumeration {
  type TimestampType = Value
  val CreateTime = Value(0, "CreateTime")
  val LogAppendTime = Value(1, "LogAppendTime")

  def getTimestampType(attribute: Byte) = {
    (attribute & Message.TimestampTypeMask) >> Message.TimestampTypeAttributeBitOffset match {
      case 0 => CreateTime
      case 1 => LogAppendTime
    }
  }

  def setTimestampType(attribute: Byte, timestampType: TimestampType): Byte = {
    if (timestampType == CreateTime)
      (attribute & ~Message.TimestampTypeMask).toByte
    else
      (attribute | Message.TimestampTypeMask).toByte
  }

}
