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

package kafka.log

import java.util.Arrays
import java.security.MessageDigest
import java.nio.ByteBuffer

import kafka.utils._
import org.apache.kafka.common.record.Record
import org.apache.kafka.common.utils.{ByteUtils, Utils}

trait OffsetMap {
  def slots: Int
  def init(strategy: String)
  def put(record: Record): Boolean
  def get(key: ByteBuffer): Long
  def updateLatestOffset(offset: Long): Unit
  def clear(): Unit
  def size: Int
  def utilization: Double = size.toDouble / slots
  def latestOffset: Long
}

object Constants {
  val OffsetStrategy: String = Defaults.CompactionStrategy
  val TimestampStrategy: String = "timestamp"
}

/**
 * An hash table used for deduplicating the log. This hash table uses a cryptographicly secure hash of the key as a proxy for the key
 * for comparisons and to save space on object overhead. Collisions are resolved by probing. This hash table does not support deletes.
 * @param memory The amount of memory this map can use
 * @param hashAlgorithm The hash algorithm instance to use: MD2, MD5, SHA-1, SHA-256, SHA-384, SHA-512
 */
@nonthreadsafe
class SkimpyOffsetMap(val memory: Int, val hashAlgorithm: String = "MD5") extends OffsetMap {
  private val bytes = ByteBuffer.allocate(memory)
  
  /* the hash algorithm instance to use, default is MD5 */
  private val digest = MessageDigest.getInstance(hashAlgorithm)
  
  /* the number of bytes for this hash algorithm */
  private val hashSize = digest.getDigestLength
  
  /* create some hash buffers to avoid reallocating each time */
  private val hash1 = new Array[Byte](hashSize)
  private val hash2 = new Array[Byte](hashSize)
  
  /* number of entries put into the map */
  private var entries = 0
  
  /* number of lookups on the map */
  private var lookups = 0L
  
  /* the number of probes for all lookups */
  private var probes = 0L

  /* the latest offset written into the map */
  private var lastOffset = -1L

  /**
   * The number of bytes of space each entry uses (the number of bytes in the hash plus an 8 byte offset)
   * This evaluates to the number of bytes in the hash plus 8 bytes for the offset
   * and, if applicable, another 8 bytes for non-offset compact strategy (set in the init method).
   */
  var bytesPerEntry = hashSize + 8
  
  /**
   * The maximum number of entries this map can contain
   */
  var slots: Int = memory / bytesPerEntry

  /* compact strategy */
  private var strategy: String = ""

  /* compact using offset strategy */
  private var isOffsetStrategy: Boolean = false

  /* compact using timestamp strategy */
  private var isTimestampStrategy: Boolean = false

  override def init(strategy: String) {
    this.strategy = strategy
    this.isOffsetStrategy = strategy == null || Constants.OffsetStrategy.equalsIgnoreCase(strategy)
    this.isTimestampStrategy = !isOffsetStrategy && Constants.TimestampStrategy.equalsIgnoreCase(strategy)

    if (!isOffsetStrategy) {
      // another 8 bytes for non-offset compact strategy.
      this.bytesPerEntry = hashSize + 8 + 8
      this.slots = memory / bytesPerEntry
    }
  }
  
  /**
   * Associate this offset to the given key.
   * @param key The key
   * @param offset The offset
   */
  override def put(record: Record): Boolean = {
    require(entries < slots, "Attempt to add a new entry to a full offset map.")

    val key = record.key
    val offset = record.offset
    val currValue = extractValue(record)

    lookups += 1
    hashInto(key, hash1)
    // probe until we find the first empty slot
    var attempt = 0
    var pos = positionOf(hash1, attempt)  
    while (!isEmpty(pos)) {
      bytes.position(pos)
      bytes.get(hash2)
      if (Arrays.equals(hash1, hash2)) {
        // we found an existing entry, overwrite it and return (size does not change)
        if (!isOffsetStrategy) {
          // read previous value by skipping offset
          bytes.position(bytes.position() + 8)
          if (bytes.getLong() > currValue) {
            // map already holding latest record
            lastOffset = offset
            return false
          }

          // reset the position to start of offset
          bytes.position(bytes.position() - 16)
        }

        // we found an existing entry, overwrite it and return (size does not change)
        bytes.putLong(offset)
        if (!isOffsetStrategy) {
          bytes.putLong(currValue)
        }

        lastOffset = offset
        return true
      }

      attempt += 1
      pos = positionOf(hash1, attempt)
    }
    // found an empty slot, update it--size grows by 1

    bytes.position(pos)
    bytes.put(hash1)
    bytes.putLong(offset)
    if (!isOffsetStrategy) {
      bytes.putLong(currValue)
    }

    lastOffset = offset
    entries += 1
    true
  }

  /** @return The value extracted from the record if the strategy is not offset, or -1 */
  private def extractValue(record: Record): Long = {
    if (isOffsetStrategy) {
      // offset strategy
      return -1L
    }

    if (isTimestampStrategy) {
      // record timestamp strategy
      return record.timestamp
    }

    if (record == null || record.headers() == null || record.headers().isEmpty) {
      // record header empty
      return -1L
    }

    // header value strategy
    record.headers()
      .filter(it => it.value != null && it.value.nonEmpty)
      .find(it => strategy.trim.equalsIgnoreCase(it.key.trim))
      .map(it => ByteBuffer.wrap(it.value))
      .map(it => ByteUtils.readVarlong(it))
      .getOrElse(-1L)
  }
  
  /**
   * Check that there is no entry at the given position
   */
  private def isEmpty(position: Int): Boolean = 
    bytes.getLong(position) == 0 && bytes.getLong(position + 8) == 0 && bytes.getLong(position + 16) == 0

  /**
   * Get the offset associated with this key.
   * @param key The key
   * @return The offset associated with this key or -1 if the key is not found
   */
  override def get(key: ByteBuffer): Long = {
    lookups += 1
    hashInto(key, hash1)
    // search for the hash of this key by repeated probing until we find the hash we are looking for or we find an empty slot
    var attempt = 0
    var pos = 0
    //we need to guard against attempt integer overflow if the map is full
    //limit attempt to number of slots once positionOf(..) enters linear search mode
    val maxAttempts = slots + hashSize - 4
    do {
     if(attempt >= maxAttempts)
        return -1L
      pos = positionOf(hash1, attempt)
      bytes.position(pos)
      if(isEmpty(pos))
        return -1L
      bytes.get(hash2)
      attempt += 1
    } while(!Arrays.equals(hash1, hash2))
    bytes.getLong()
  }
  
  /**
   * Change the salt used for key hashing making all existing keys unfindable.
   */
  override def clear(): Unit = {
    this.entries = 0
    this.lookups = 0L
    this.probes = 0L
    this.lastOffset = -1L
    Arrays.fill(bytes.array, bytes.arrayOffset, bytes.arrayOffset + bytes.limit(), 0.toByte)
  }
  
  /**
   * The number of entries put into the map (note that not all may remain)
   */
  override def size: Int = entries
  
  /**
   * The rate of collisions in the lookups
   */
  def collisionRate: Double = 
    (this.probes - this.lookups) / this.lookups.toDouble

  /**
   * The latest offset put into the map
   */
  override def latestOffset: Long = lastOffset

  override def updateLatestOffset(offset: Long): Unit = {
    lastOffset = offset
  }

  /**
   * Calculate the ith probe position. We first try reading successive integers from the hash itself
   * then if all of those fail we degrade to linear probing.
   * @param hash The hash of the key to find the position for
   * @param attempt The ith probe
   * @return The byte offset in the buffer at which the ith probing for the given hash would reside
   */
  private def positionOf(hash: Array[Byte], attempt: Int): Int = {
    val probe = CoreUtils.readInt(hash, math.min(attempt, hashSize - 4)) + math.max(0, attempt - hashSize + 4)
    val slot = Utils.abs(probe) % slots
    this.probes += 1
    slot * bytesPerEntry
  }
  
  /**
   * The offset at which we have stored the given key
   * @param key The key to hash
   * @param buffer The buffer to store the hash into
   */
  private def hashInto(key: ByteBuffer, buffer: Array[Byte]): Unit = {
    key.mark()
    digest.update(key)
    key.reset()
    digest.digest(buffer, 0, hashSize)
  }
  
}
