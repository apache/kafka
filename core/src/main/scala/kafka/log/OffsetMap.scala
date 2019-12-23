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
  /* The maximum number of entries this map can contain */
  def slots: Int

  /* Initialize the map with the topic compact strategy */
  def init(strategy: String, headerKey: String, cleanerThreadId: Int, topicPartition: String)

  /**
   * Associate this offset to the given key.
   * @param record The record
   * @return success flag
   */
  def put(record: Record): Boolean

  /**
   * Checks to see whether to retain the record or not
   * @param record The record
   * @return true to retain; false not to
   */
  def shouldRetainRecord(record: Record): Boolean

  /**
   * Get the offset associated with this key.
   * @param key The key
   * @return The offset associated with this key or -1 if the key is not found
   */
  def get(key: ByteBuffer): Long

  /**
   * Get the version associated with this key for non-offset based strategy.
   * @param key The key
   * @return The version associated with this key or -1 if the key is not found
   */
  def getVersion(key: ByteBuffer): Long

  /**
   * Sets the passed value as the latest offset.
   * @param offset teh latest offset
   */
  def updateLatestOffset(offset: Long): Unit

  /* The number of entries put into the map (note that not all may remain) */
  def size: Int

  def utilization: Double = size.toDouble / slots

  /* The latest offset put into the map */
  def latestOffset: Long
}

/**
 * An hash table used for deduplicating the log. This hash table uses a cryptographicly secure hash of the key as a proxy for the key
 * for comparisons and to save space on object overhead. Collisions are resolved by probing. This hash table does not support deletes.
 * @param memory The amount of memory this map can use
 * @param hashAlgorithm The hash algorithm instance to use: MD2, MD5, SHA-1, SHA-256, SHA-384, SHA-512
 */
@nonthreadsafe
class SkimpyOffsetMap(val memory: Int, val hashAlgorithm: String = "MD5") extends OffsetMap with Logging {
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

  /* compact using offset strategy */
  private var isOffsetStrategy: Boolean = false

  /* compact using timestamp strategy */
  private var isTimestampStrategy: Boolean = false

  /* compact using header strategy */
  private var isHeaderStrategy: Boolean = false

  /* header key for the Strategy header to look for */
  private var headerKey: String = ""

  /**
   * Initialize the map with the topic compact strategy
   * @param strategy The compaction strategy
   * @param headerKey The header key if the compaction strategy is set to header
   * @param cleanerThreadId The clenaer thread id
   * @param topicPartitionName The topic partition name
   */
  override def init(strategy: String = Defaults.CompactionStrategy, headerKey: String = "", cleanerThreadId: Int = -1, topicPartitionName: String = "") {
    // set the log indent for the topic partition
    this.logIdent = s"[OffsetMap-$cleanerThreadId $topicPartitionName]: "
    info(s"Initializing OffsetMap with compaction strategy '$strategy' and header key '$headerKey'")

    // Change the salt used for key hashing making all existing keys unfindable.
    this.entries = 0
    this.lookups = 0L
    this.probes = 0L
    this.lastOffset = -1L
    Arrays.fill(bytes.array, bytes.arrayOffset, bytes.arrayOffset + bytes.limit(), 0.toByte)

    // reset all strategy flags
    this.isOffsetStrategy = false
    this.isTimestampStrategy = false
    this.isHeaderStrategy = false

    if (strategy != null && Defaults.CompactionStrategyTimestamp.equalsIgnoreCase(strategy)) {
      // timestamp strategy
      this.isOffsetStrategy = false
      this.isTimestampStrategy = true
      this.isHeaderStrategy = false
      this.headerKey = ""
      info("Compaction strategy set to 'timestamp'")
    } else if (strategy != null && Defaults.CompactionStrategyHeader.equalsIgnoreCase(strategy) && !headerKey.trim().isEmpty()) {
      // header strategy
      this.isOffsetStrategy = false
      this.isTimestampStrategy = false
      this.isHeaderStrategy = true
      this.headerKey = headerKey.trim()
      info(s"Compaction strategy set to 'header' with header key as '$headerKey''")
    }

    if (!this.isTimestampStrategy && !this.isHeaderStrategy)
    {
      // make it as offset strategy for anything else
      //   - offset strategy set explictly 
      //   - missing broker/topic level strategy config
      //   - doesn't fall under timestamp/header sequence
      this.isOffsetStrategy = true
      this.isTimestampStrategy = false
      this.isHeaderStrategy = false
      this.headerKey = ""
      info("Compaction strategy set to 'offset'")
    }

    this.bytesPerEntry = hashSize + 8 + (if (isOffsetStrategy) 0 else 8)
    this.slots = memory / bytesPerEntry
  }
  
  /**
   * Associate this offset to the given key.
   * @param record The record
   * @return success flag
   */
  override def put(record: Record): Boolean = {
    require(entries < slots, "Attempt to add a new entry to a full offset map.")

    val key = record.key
    val offset = record.offset
    val currVersion = extractVersion(record)

    lookups += 1
    hashInto(key, hash1)

    val b = new Array[Byte](key.limit())
    key.get(b)
    val keyStr = new String(b, "UTF-8")
    info(s"Processing key: $keyStr with offset: $offset and curr value: $currVersion")

    // probe until we find the first empty slot
    var attempt = 0
    var pos = positionOf(hash1, attempt)  
    while (!isEmpty(pos)) {
      bytes.position(pos)
      bytes.get(hash2)
      info(s"bytes attempt: $attempt; pos: $pos; hash1: [" + hash1.mkString(" ") + "]; hash2: [" + hash2.mkString(" ") + "]")
      if (Arrays.equals(hash1, hash2)) {
        info("keys are same...")
        // we found an existing entry, overwrite it and return (size does not change)
        if (!isOffsetStrategy) {
          // read previous value by skipping offset
          bytes.position(bytes.position() + 8)
          val foundVersion = bytes.getLong()
          if (foundVersion > currVersion) {
            info(s"map already holding latest value for the key $keyStr; offset: $offset; found version: $foundVersion; current version: $currVersion")
            // map already holding latest record
            lastOffset = offset
            return false
          }

          // reset the position to start of offset
          bytes.position(bytes.position() - 16)
        }

        // we found an existing entry, overwrite it and return (size does not change)
        info(s"overriding existing key $keyStr; offset: $offset; curr value: $currVersion")
        bytes.putLong(offset)
        if (!isOffsetStrategy) {
          bytes.putLong(currVersion)
        }

        lastOffset = offset
        return true
      }

      attempt += 1
      pos = positionOf(hash1, attempt)
    }

    // found an empty slot, update it--size grows by 1
    info(s"Adding new key $keyStr; offset: $offset; curr value: $currVersion")
    bytes.position(pos)
    bytes.put(hash1)
    bytes.putLong(offset)
    if (!isOffsetStrategy) {
      bytes.putLong(currVersion)
    }

    lastOffset = offset
    entries += 1
    true
  }

  /**
   * Check that there is no entry at the given position
   */
  private def isEmpty(position: Int): Boolean = 
    bytes.getLong(position) == 0 && bytes.getLong(position + 8) == 0 && bytes.getLong(position + 16) == 0

  /**
   * Checks to see whether to retain the record or not
   * @param record The record
   * @return true to retain; false not to
   */
  override def shouldRetainRecord(record: Record): Boolean = {
    val key = record.key
    val foundOffset = get(key)
    if (isOffsetStrategy) {
      record.offset() >= foundOffset
    } else {
      val foundVersion = getVersion(record.key)
      val currentVersion = extractVersion(record)
      currentVersion >= foundVersion
    }
  }

  /**
   * Get the offset/version associated with this key.
   * @param key The key
   * @return The offset/version associated with this key or -1 if the key is not found
   */
  override def get(key: ByteBuffer): Long = {
    // search for the hash of this key by repeated probing until we find the hash we are looking for or we find an empty slot
    if (!search(key))
      return -1L

    bytes.getLong()
  }

  /**
   * Get the version associated with this key for the non-offset based strategy.
   * @param key The key
   * @return The version associated with this key or -1 if the key is not found
   */
  override def getVersion(key: ByteBuffer): Long = {
    if (isOffsetStrategy)
      return -1L

    // search for the hash of this key by repeated probing until we find the hash we are looking for or we find an empty slot
    if (!search(key))
      return -1L

    // for non-offset based strategy skipping offset
    bytes.position(bytes.position() + 8)
    bytes.getLong()
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
   * Search for the hash of the key by repeated probing until we find the hash we are looking for or we find an empty slot
   * @param key The key
   * @return true if key found otherwise false
   */
  private def search(key: ByteBuffer): Boolean = {
    lookups += 1
    hashInto(key, hash1)

    var attempt = 0
    var pos = 0
    //we need to guard against attempt integer overflow if the map is full
    //limit attempt to number of slots once positionOf(..) enters linear search mode
    val maxAttempts = slots + hashSize - 4
    do {
      if(attempt >= maxAttempts)
        return false

      pos = positionOf(hash1, attempt)
      bytes.position(pos)
      if(isEmpty(pos))
        return false

      bytes.get(hash2)
      attempt += 1
    } while(!Arrays.equals(hash1, hash2))

    // found
    true
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

  /**
   * Extract the version for the non-offset based compact strategy for comparison
   * @param record The record
   * @return The version extracted from the record if the strategy is not offset, or -1
   */
  private def extractVersion(record: Record): Long = {
    if (isOffsetStrategy) {
      // offset strategy
      return -1L
    }

    if (isTimestampStrategy) {
      // record timestamp strategy
      return record.timestamp
    }

    // header strategy
    if (record == null || record.headers() == null || record.headers().isEmpty) {
      // record header empty
      info("Empty Headers, returning -1L for the record offset: %d".format(record.offset()))
      return -1L
    }

    val headerValue = record.headers()
      .filter(it => it.value != null && it.value.nonEmpty)
      .find(it => headerKey.equalsIgnoreCase(it.key.trim))
      .map(it => ByteBuffer.wrap(it.value))
      .map(it => ByteUtils.readVarlong(it))
      .getOrElse(-1L)

    if (headerValue == -1L) {
      info(s"Either header key '%s' missing or set to -1L for the record offset: %d".format(headerValue, record.offset()))
    }

    headerValue
  }
}
