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

import scala.math._
import java.io._
import java.nio._
import java.nio.channels._
import java.util.concurrent.atomic._
import kafka.utils._
import kafka.common.InvalidOffsetException

/**
 * An index that maps offsets to physical file locations for a particular log segment. This index may be sparse:
 * that is it may not hold an entry for all messages in the log.
 * 
 * The index is stored in a file that is pre-allocated to hold a fixed maximum number of 8-byte entries.
 * 
 * The index supports lookups against a memory-map of this file. These lookups are done using a simple binary search variant
 * to locate the offset/location pair for the greatest offset less than or equal to the target offset.
 * 
 * Index files can be opened in two ways: either as an empty, mutable index that allows appends or
 * an immutable read-only index file that has previously been populated. The makeReadOnly method will turn a mutable file into an 
 * immutable one and truncate off any extra bytes. This is done when the index file is rolled over.
 * 
 * No attempt is made to checksum the contents of this file, in the event of a crash it is rebuilt.
 * 
 * The file format is a series of entries. The physical format is a 4 byte "relative" offset and a 4 byte file location for the 
 * message with that offset. The offset stored is relative to the base offset of the index file. So, for example,
 * if the base offset was 50, then the offset 55 would be stored as 5. Using relative offsets in this way let's us use
 * only 4 bytes for the offset.
 * 
 * The frequency of entries is up to the user of this class.
 * 
 * All external APIs translate from relative offsets to full offsets, so users of this class do not interact with the internal 
 * storage format.
 */
class OffsetIndex(val file: File, val baseOffset: Long, val maxIndexSize: Int = -1) extends Logging {
  
  /* the memory mapping */
  private var mmap: MappedByteBuffer = 
    {
      val newlyCreated = file.createNewFile()
      val raf = new RandomAccessFile(file, "rw")
      try {
        /* pre-allocate the file if necessary */
        if(newlyCreated) {
          if(maxIndexSize < 8)
            throw new IllegalArgumentException("Invalid max index size: " + maxIndexSize)
          raf.setLength(roundToExactMultiple(maxIndexSize, 8))
        }
          
        val len = raf.length()  
        if(len < 0 || len % 8 != 0)
          throw new IllegalStateException("Index file " + file.getName + " is corrupt, found " + len + 
                                          " bytes which is not positive or not a multiple of 8.")
          
        /* memory-map the file */
        val idx = raf.getChannel.map(FileChannel.MapMode.READ_WRITE, 0, len)
          
        /* set the position in the index for the next entry */
        if(newlyCreated)
          idx.position(0)
        else
          // if this is a pre-existing index, assume it is all valid and set position to last entry
          idx.position(roundToExactMultiple(idx.limit, 8))
        idx
      } finally {
        Utils.swallow(raf.close())
      }
    }

  /* the number of entries in the index */
  private var size = new AtomicInteger(mmap.position / 8)
  
  /* the last offset in the index */
  var lastOffset = readLastOffset()
  
  debug("Loaded index file %s with maxEntries = %d, maxIndexSize = %d, entries = %d, lastOffset = %d, file position = %d"
    .format(file.getAbsolutePath, maxEntries, maxIndexSize, entries(), lastOffset, mmap.position))

  /* the maximum number of entries this index can hold */
  def maxEntries = mmap.limit / 8

  /**
   * The last offset written to the index
   */
  private def readLastOffset(): Long = {
    val offset = 
      size.get match {
        case 0 => 0
        case s => relativeOffset(this.mmap, s-1)
      }
    baseOffset + offset
  }

  /**
   * Find the largest offset less than or equal to the given targetOffset 
   * and return a pair holding this offset and it's corresponding physical file position.
   * If the target offset is smaller than the least entry in the index (or the index is empty),
   * the pair (baseOffset, 0) is returned.
   */
  def lookup(targetOffset: Long): OffsetPosition = {
    val idx = mmap.duplicate
    val slot = indexSlotFor(idx, targetOffset)
    if(slot == -1)
      OffsetPosition(baseOffset, 0)
    else
      OffsetPosition(baseOffset + relativeOffset(idx, slot), physical(idx, slot))
  }
  
  /**
   * Find the slot in which the largest offset less than or equal to the given
   * target offset is stored.
   * Return -1 if the least entry in the index is larger than the target offset or the index is empty
   */
  private def indexSlotFor(idx: ByteBuffer, targetOffset: Long): Int = {
    // we only store the difference from the base offset so calculate that
    val relOffset = targetOffset - baseOffset
    
    // check if the index is empty
    if(entries == 0)
      return -1
    
    // check if the target offset is smaller than the least offset
    if(relativeOffset(idx, 0) > relOffset)
      return -1
      
    // binary search for the entry
    var lo = 0
    var hi = entries-1
    while(lo < hi) {
      val mid = ceil(hi/2.0 + lo/2.0).toInt
      val found = relativeOffset(idx, mid)
      if(found == relOffset)
        return mid
      else if(found < relOffset)
        lo = mid
      else
        hi = mid - 1
    }
    lo
  }
  
  /* return the nth offset relative to the base offset */
  private def relativeOffset(buffer: ByteBuffer, n: Int): Int = buffer.getInt(n * 8)
  
  /* return the nth physical offset */
  private def physical(buffer: ByteBuffer, n: Int): Int = buffer.getInt(n * 8 + 4)
  
  /**
   * Get the nth offset mapping from the index
   */
  def entry(n: Int): OffsetPosition = {
    if(n >= entries)
      throw new IllegalArgumentException("Attempt to fetch the %dth entry from an index of size %d.".format(n, entries))
    val idx = mmap.duplicate
    OffsetPosition(relativeOffset(idx, n), physical(idx, n))
  }
  
  /**
   * Append entry for the given offset/location pair to the index. This entry must have a larger offset than all subsequent entries.
   */
  def append(offset: Long, position: Int) {
    this synchronized {
      require(!isFull, "Attempt to append to a full index (size = " + size + ").")
      if (size.get == 0 || offset > lastOffset) {
        debug("Adding index entry %d => %d to %s.".format(offset, position, file.getName))
        this.mmap.putInt((offset - baseOffset).toInt)
        this.mmap.putInt(position)
        this.size.incrementAndGet()
        this.lastOffset = offset
        require(entries * 8 == mmap.position, entries + " entries but file position in index is " + mmap.position + ".")
      }
      else {
        throw new InvalidOffsetException("Attempt to append an offset (%d) to position %d no larger than the last offset appended (%d) to %s."
          .format(offset, entries, lastOffset, file.getName))
      }
    }
  }
  
  /**
   * True iff there are no more slots available in this index
   */
  def isFull: Boolean = entries >= this.maxEntries
  
  /**
   * Truncate the entire index
   */
  def truncate() = truncateToEntries(0)
  
  /**
   * Remove all entries from the index which have an offset greater than or equal to the given offset.
   * Truncating to an offset larger than the largest in the index has no effect.
   */
  def truncateTo(offset: Long) {
    this synchronized {
      val idx = mmap.duplicate
      val slot = indexSlotFor(idx, offset)

      /* There are 3 cases for choosing the new size
       * 1) if there is no entry in the index <= the offset, delete everything
       * 2) if there is an entry for this exact offset, delete it and everything larger than it
       * 3) if there is no entry for this offset, delete everything larger than the next smallest
       */
      val newEntries = 
        if(slot < 0)
          0
        else if(relativeOffset(idx, slot) == offset - baseOffset)
          slot
        else
          slot + 1
      truncateToEntries(newEntries)
    }
  }

  /**
   * Truncates index to a known number of entries.
   */
  private def truncateToEntries(entries: Int) {
    this.size.set(entries)
    mmap.position(this.size.get * 8)
    this.lastOffset = readLastOffset
  }
  
  /**
   * Trim this segment to fit just the valid entries, deleting all trailing unwritten bytes from
   * the file.
   */
  def trimToValidSize() {
    this synchronized {
      resize(entries * 8)
    }
  }

  /**
   * Reset the size of the memory map and the underneath file. This is used in two kinds of cases: (1) in
   * trimToValidSize() which is called at closing the segment or new segment being rolled; (2) at
   * loading segments from disk or truncating back to an old segment where a new log segment became active;
   * we want to reset the index size to maximum index size to avoid rolling new segment.
   */
  def resize(newSize: Int) {
    this synchronized {
      flush()
      val raf = new RandomAccessFile(file, "rws")
      val roundedNewSize = roundToExactMultiple(newSize, 8)
      try {
        raf.setLength(roundedNewSize)
        val position = this.mmap.position
        this.mmap = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, roundedNewSize)
        this.mmap.position(position)
      } finally {
        Utils.swallow(raf.close())
      }
    }
  }
  
  /**
   * Flush the data in the index to disk
   */
  def flush() {
    this synchronized {
      mmap.force()
    }
  }
  
  /**
   * Delete this index file
   */
  def delete(): Boolean = {
    info("Deleting index " + this.file.getAbsolutePath)
    this.file.delete()
  }
  
  /** The number of entries in this index */
  def entries() = size.get
  
  /** Close the index */
  def close() {
    trimToValidSize()
  }
  
  /**
   * Round a number to the greatest exact multiple of the given factor less than the given number.
   * E.g. roundToExactMultiple(67, 8) == 64
   */
  private def roundToExactMultiple(number: Int, factor: Int) = factor * (number / factor)
}