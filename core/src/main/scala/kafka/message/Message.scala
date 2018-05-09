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

import java.nio._

import org.apache.kafka.common.record.{CompressionType, LegacyRecord, TimestampType}

import scala.math._
import org.apache.kafka.common.utils.{ByteUtils, Crc32}

/**
 * Constants related to messages
 */
object Message {

  /**
   * The current offset and size for all the fixed-length fields
   */
  val CrcOffset = 0
  val CrcLength = 4
  val MagicOffset = CrcOffset + CrcLength
  val MagicLength = 1
  val AttributesOffset = MagicOffset + MagicLength
  val AttributesLength = 1
  // Only message format version 1 has the timestamp field.
  val TimestampOffset = AttributesOffset + AttributesLength
  val TimestampLength = 8
  val KeySizeOffset_V0 = AttributesOffset + AttributesLength
  val KeySizeOffset_V1 = TimestampOffset + TimestampLength
  val KeySizeLength = 4
  val KeyOffset_V0 = KeySizeOffset_V0 + KeySizeLength
  val KeyOffset_V1 = KeySizeOffset_V1 + KeySizeLength
  val ValueSizeLength = 4

  private val MessageHeaderSizeMap = Map (
    (0: Byte) -> (CrcLength + MagicLength + AttributesLength + KeySizeLength + ValueSizeLength),
    (1: Byte) -> (CrcLength + MagicLength + AttributesLength + TimestampLength + KeySizeLength + ValueSizeLength))

  /**
   * The amount of overhead bytes in a message
   * This value is only used to check if the message size is valid or not. So the minimum possible message bytes is
   * used here, which comes from a message in message format V0 with empty key and value.
   */
  val MinMessageOverhead = KeyOffset_V0 + ValueSizeLength
  
  /**
   * The "magic" value
   * When magic value is 0, the message uses absolute offset and does not have a timestamp field.
   * When magic value is 1, the message uses relative offset and has a timestamp field.
   */
  val MagicValue_V0: Byte = 0
  val MagicValue_V1: Byte = 1
  val CurrentMagicValue: Byte = 1

  /**
   * Specifies the mask for the compression code. 3 bits to hold the compression codec.
   * 0 is reserved to indicate no compression
   */
  val CompressionCodeMask: Int = 0x07
  /**
   * Specifies the mask for timestamp type. 1 bit at the 4th least significant bit.
   * 0 for CreateTime, 1 for LogAppendTime
   */
  val TimestampTypeMask: Byte = 0x08
  val TimestampTypeAttributeBitOffset: Int = 3

  /**
   * Compression code for uncompressed messages
   */
  val NoCompression: Int = 0

  /**
   * To indicate timestamp is not defined so "magic" value 0 will be used.
   */
  val NoTimestamp: Long = -1

  /**
   * Give the header size difference between different message versions.
   */
  def headerSizeDiff(fromMagicValue: Byte, toMagicValue: Byte) : Int =
    MessageHeaderSizeMap(toMagicValue) - MessageHeaderSizeMap(fromMagicValue)


  def fromRecord(record: LegacyRecord): Message = {
    val wrapperTimestamp: Option[Long] = if (record.wrapperRecordTimestamp == null) None else Some(record.wrapperRecordTimestamp)
    val wrapperTimestampType = Option(record.wrapperRecordTimestampType)
    new Message(record.buffer, wrapperTimestamp, wrapperTimestampType)
  }
}

/**
 * A message. The format of an N byte message is the following:
 *
 * 1. 4 byte CRC32 of the message
 * 2. 1 byte "magic" identifier to allow format changes, value is 0 or 1
 * 3. 1 byte "attributes" identifier to allow annotations on the message independent of the version
 *    bit 0 ~ 2 : Compression codec.
 *      0 : no compression
 *      1 : gzip
 *      2 : snappy
 *      3 : lz4
 *    bit 3 : Timestamp type
 *      0 : create time
 *      1 : log append time
 *    bit 4 ~ 7 : reserved
 * 4. (Optional) 8 byte timestamp only if "magic" identifier is greater than 0
 * 5. 4 byte key length, containing length K
 * 6. K byte key
 * 7. 4 byte payload length, containing length V
 * 8. V byte payload
 *
 * Default constructor wraps an existing ByteBuffer with the Message object with no change to the contents.
 * @param buffer the byte buffer of this message.
 * @param wrapperMessageTimestamp the wrapper message timestamp, which is only defined when the message is an inner
 *                                message of a compressed message.
 * @param wrapperMessageTimestampType the wrapper message timestamp type, which is only defined when the message is an
 *                                    inner message of a compressed message.
 */
class Message(val buffer: ByteBuffer,
              private val wrapperMessageTimestamp: Option[Long] = None,
              private val wrapperMessageTimestampType: Option[TimestampType] = None) {
  
  import kafka.message.Message._

  private[message] def asRecord: LegacyRecord = wrapperMessageTimestamp match {
    case None => new LegacyRecord(buffer)
    case Some(timestamp) => new LegacyRecord(buffer, timestamp, wrapperMessageTimestampType.orNull)
  }

  /**
   * A constructor to create a Message
   * @param bytes The payload of the message
   * @param key The key of the message (null, if none)
   * @param timestamp The timestamp of the message.
   * @param timestampType The timestamp type of the message.
   * @param codec The compression codec used on the contents of the message (if any)
   * @param payloadOffset The offset into the payload array used to extract payload
   * @param payloadSize The size of the payload to use
   * @param magicValue the magic value to use
   */
  def this(bytes: Array[Byte], 
           key: Array[Byte],
           timestamp: Long,
           timestampType: TimestampType,
           codec: CompressionCodec, 
           payloadOffset: Int, 
           payloadSize: Int,
           magicValue: Byte) = {
    this(ByteBuffer.allocate(Message.CrcLength +
                             Message.MagicLength +
                             Message.AttributesLength +
                             (if (magicValue == Message.MagicValue_V0) 0
                              else Message.TimestampLength) +
                             Message.KeySizeLength + 
                             (if(key == null) 0 else key.length) + 
                             Message.ValueSizeLength + 
                             (if(bytes == null) 0 
                              else if(payloadSize >= 0) payloadSize 
                              else bytes.length - payloadOffset)))
    validateTimestampAndMagicValue(timestamp, magicValue)
    // skip crc, we will fill that in at the end
    buffer.position(MagicOffset)
    buffer.put(magicValue)
    val attributes: Byte = LegacyRecord.computeAttributes(magicValue, CompressionType.forId(codec.codec), timestampType)
    buffer.put(attributes)
    // Only put timestamp when "magic" value is greater than 0
    if (magic > MagicValue_V0)
      buffer.putLong(timestamp)
    if(key == null) {
      buffer.putInt(-1)
    } else {
      buffer.putInt(key.length)
      buffer.put(key, 0, key.length)
    }
    val size = if(bytes == null) -1
               else if(payloadSize >= 0) payloadSize 
               else bytes.length - payloadOffset
    buffer.putInt(size)
    if(bytes != null)
      buffer.put(bytes, payloadOffset, size)
    buffer.rewind()

    // now compute the checksum and fill it in
    ByteUtils.writeUnsignedInt(buffer, CrcOffset, computeChecksum)
  }

  def this(bytes: Array[Byte], key: Array[Byte], timestamp: Long, timestampType: TimestampType, codec: CompressionCodec, magicValue: Byte) =
    this(bytes = bytes, key = key, timestamp = timestamp, timestampType = timestampType, codec = codec, payloadOffset = 0, payloadSize = -1, magicValue = magicValue)

  def this(bytes: Array[Byte], key: Array[Byte], timestamp: Long, codec: CompressionCodec, magicValue: Byte) =
    this(bytes = bytes, key = key, timestamp = timestamp, timestampType = TimestampType.CREATE_TIME, codec = codec, payloadOffset = 0, payloadSize = -1, magicValue = magicValue)
  
  def this(bytes: Array[Byte], timestamp: Long, codec: CompressionCodec, magicValue: Byte) =
    this(bytes = bytes, key = null, timestamp = timestamp, codec = codec, magicValue = magicValue)
  
  def this(bytes: Array[Byte], key: Array[Byte], timestamp: Long, magicValue: Byte) =
    this(bytes = bytes, key = key, timestamp = timestamp, codec = NoCompressionCodec, magicValue = magicValue)
    
  def this(bytes: Array[Byte], timestamp: Long, magicValue: Byte) =
    this(bytes = bytes, key = null, timestamp = timestamp, codec = NoCompressionCodec, magicValue = magicValue)

  def this(bytes: Array[Byte]) =
    this(bytes = bytes, key = null, timestamp = Message.NoTimestamp, codec = NoCompressionCodec, magicValue = Message.CurrentMagicValue)
    
  /**
   * Compute the checksum of the message from the message contents
   */
  def computeChecksum: Long =
    Crc32.crc32(buffer, MagicOffset, buffer.limit() - MagicOffset)
  
  /**
   * Retrieve the previously computed CRC for this message
   */
  def checksum: Long = ByteUtils.readUnsignedInt(buffer, CrcOffset)
  
    /**
   * Returns true if the crc stored with the message matches the crc computed off the message contents
   */
  def isValid: Boolean = checksum == computeChecksum
  
  /**
   * Throw an InvalidMessageException if isValid is false for this message
   */
  def ensureValid() {
    if(!isValid)
      throw new InvalidMessageException(s"Message is corrupt (stored crc = ${checksum}, computed crc = ${computeChecksum})")
  }
  
  /**
   * The complete serialized size of this message in bytes (including crc, header attributes, etc)
   */
  def size: Int = buffer.limit()

  /**
   * The position where the key size is stored.
   */
  private def keySizeOffset = {
    if (magic == MagicValue_V0) KeySizeOffset_V0
    else KeySizeOffset_V1
  }

  /**
   * The length of the key in bytes
   */
  def keySize: Int = buffer.getInt(keySizeOffset)
  
  /**
   * Does the message have a key?
   */
  def hasKey: Boolean = keySize >= 0
  
  /**
   * The position where the payload size is stored
   */
  private def payloadSizeOffset = {
    if (magic == MagicValue_V0) KeyOffset_V0 + max(0, keySize)
    else KeyOffset_V1 + max(0, keySize)
  }
  
  /**
   * The length of the message value in bytes
   */
  def payloadSize: Int = buffer.getInt(payloadSizeOffset)
  
  /**
   * Is the payload of this message null
   */
  def isNull: Boolean = payloadSize < 0
  
  /**
   * The magic version of this message
   */
  def magic: Byte = buffer.get(MagicOffset)
  
  /**
   * The attributes stored with this message
   */
  def attributes: Byte = buffer.get(AttributesOffset)

  /**
   * The timestamp of the message, only available when the "magic" value is greater than 0
   * When magic > 0, The timestamp of a message is determined in the following way:
   * 1. wrapperMessageTimestampType = None and wrapperMessageTimestamp is None - Uncompressed message, timestamp and timestamp type are in the message.
   * 2. wrapperMessageTimestampType = LogAppendTime and wrapperMessageTimestamp is defined - Compressed message using LogAppendTime
   * 3. wrapperMessageTimestampType = CreateTime and wrapperMessageTimestamp is defined - Compressed message using CreateTime
   */
  def timestamp: Long = {
    if (magic == MagicValue_V0)
      Message.NoTimestamp
    // Case 2
    else if (wrapperMessageTimestampType.exists(_ == TimestampType.LOG_APPEND_TIME) && wrapperMessageTimestamp.isDefined)
      wrapperMessageTimestamp.get
    else // case 1, 3
      buffer.getLong(Message.TimestampOffset)
  }

  /**
   * The timestamp type of the message
   */
  def timestampType = LegacyRecord.timestampType(magic, wrapperMessageTimestampType.orNull, attributes)

  /**
   * The compression codec used with this message
   */
  def compressionCodec: CompressionCodec = 
    CompressionCodec.getCompressionCodec(buffer.get(AttributesOffset) & CompressionCodeMask)
  
  /**
   * A ByteBuffer containing the content of the message
   */
  def payload: ByteBuffer = sliceDelimited(payloadSizeOffset)
  
  /**
   * A ByteBuffer containing the message key
   */
  def key: ByteBuffer = sliceDelimited(keySizeOffset)

  /**
   * Read a size-delimited byte buffer starting at the given offset
   */
  private def sliceDelimited(start: Int): ByteBuffer = {
    val size = buffer.getInt(start)
    if(size < 0) {
      null
    } else {
      var b = buffer.duplicate()
      b.position(start + 4)
      b = b.slice()
      b.limit(size)
      b.rewind
      b
    }
  }

  /**
   * Validate the timestamp and "magic" value
   */
  private def validateTimestampAndMagicValue(timestamp: Long, magic: Byte) {
    if (magic != MagicValue_V0 && magic != MagicValue_V1)
      throw new IllegalArgumentException(s"Invalid magic value $magic")
    if (timestamp < 0 && timestamp != NoTimestamp)
      throw new IllegalArgumentException(s"Invalid message timestamp $timestamp")
    if (magic == MagicValue_V0 && timestamp != NoTimestamp)
      throw new IllegalArgumentException(s"Invalid timestamp $timestamp. Timestamp must be $NoTimestamp when magic = $MagicValue_V0")
  }

  override def toString: String = {
    if (magic == MagicValue_V0)
      s"Message(magic = $magic, attributes = $attributes, crc = $checksum, key = $key, payload = $payload)"
    else
      s"Message(magic = $magic, attributes = $attributes, $timestampType = $timestamp, crc = $checksum, key = $key, payload = $payload)"
  }

  override def equals(any: Any): Boolean = {
    any match {
      case that: Message => this.buffer.equals(that.buffer)
      case _ => false
    }
  }
  
  override def hashCode(): Int = buffer.hashCode

}
