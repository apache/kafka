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

trait OffsetMap {
  def capacity: Int
  def put(key: ByteBuffer, offset: Long)
  def get(key: ByteBuffer): Long
  def clear()
  def size: Int
  def utilization: Double = size.toDouble / capacity
}

/**
 * An approximate map used for deduplicating the log.
 * @param memory The amount of memory this map can use
 * @param maxLoadFactor The maximum percent full this offset map can be
 * @param hashAlgorithm The hash algorithm instance to use: MD2, MD5, SHA-1, SHA-256, SHA-384, SHA-512
 */
@nonthreadsafe
class SkimpyOffsetMap(val memory: Int, val maxLoadFactor: Double, val hashAlgorithm: String = "MD5") extends OffsetMap {
  private val bytes = ByteBuffer.allocate(memory)
  
  /* the hash algorithm instance to use, defualt is MD5 */
  private val digest = MessageDigest.getInstance(hashAlgorithm)
  
  /* the number of bytes for this hash algorithm */
  private val hashSize = digest.getDigestLength
  
  /* create some hash buffers to avoid reallocating each time */
  private val hash1 = new Array[Byte](hashSize)
  private val hash2 = new Array[Byte](hashSize)
  
  /* number of entries put into the map */
  private var entries = 0
  
  /* a byte added as a prefix to all keys to make collisions non-static in repeated uses. Changed in clear(). */
  private var salt: Byte = 0
  
  /**
   * The number of bytes of space each entry uses (the number of bytes in the hash plus an 8 byte offset)
   */
  val bytesPerEntry = hashSize + 8
  
  /**
   * The maximum number of entries this map can contain before it exceeds the max load factor
   */
  override val capacity: Int = (maxLoadFactor * memory / bytesPerEntry).toInt
  
  /**
   * Associate a offset with a key.
   * @param key The key
   * @param offset The offset
   */
  override def put(key: ByteBuffer, offset: Long) {
    if(size + 1 > capacity)
      throw new IllegalStateException("Attempt to add to a full offset map with a maximum capacity of %d.".format(capacity))
    hash(key, hash1)
    bytes.position(offsetFor(hash1))
    bytes.put(hash1)
    bytes.putLong(offset)
    entries += 1
  }
  
  /**
   * Get the offset associated with this key. This method is approximate,
   * it may not find an offset previously stored, but cannot give a wrong offset.
   * @param key The key
   * @return The offset associated with this key or -1 if the key is not found
   */
  override def get(key: ByteBuffer): Long = {
    hash(key, hash1)
    bytes.position(offsetFor(hash1))
    bytes.get(hash2)
    // if the computed hash equals the stored hash return the stored offset
    if(Arrays.equals(hash1, hash2))
      bytes.getLong()
    else
      -1L
  }
  
  /**
   * Change the salt used for key hashing making all existing keys unfindable.
   * Doesn't actually zero out the array.
   */
  override def clear() {
    this.entries = 0
    this.salt = (this.salt + 1).toByte
    Arrays.fill(bytes.array, bytes.arrayOffset, bytes.arrayOffset + bytes.limit, 0.toByte)
  }
  
  /**
   * The number of entries put into the map (note that not all may remain)
   */
  override def size: Int = entries
  
  /**
   * Choose a slot in the array for this hash
   */
  private def offsetFor(hash: Array[Byte]): Int = 
    bytesPerEntry * (Utils.abs(Utils.readInt(hash, 0)) % capacity)
  
  /**
   * The offset at which we have stored the given key
   * @param key The key to hash
   * @param buffer The buffer to store the hash into
   */
  private def hash(key: ByteBuffer, buffer: Array[Byte]) {
    key.mark()
    digest.update(salt)
    digest.update(key)
    key.reset()
    digest.digest(buffer, 0, hashSize)
  }
  
}