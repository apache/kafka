package kafka.log

import scala.math._
import java.io.File
import kafka.common._
import kafka.message._
import kafka.utils.{Utils, Range, Time, SystemTime, nonthreadsafe}

/**
 * A segment of the log. Each segment has two components: a log and an index. The log is a FileMessageSet containing
 * the actual messages. The index is an OffsetIndex that maps from logical offsets to physical file positions. Each 
 * segment has a base offset which is an offset <= the least offset of any message in this segment and > any offset in
 * any previous segment.
 * 
 * A segment with a base offset of [base_offset] would be stored in two files, a [base_offset].index and a [base_offset].log file. 
 */
@nonthreadsafe
class LogSegment(val messageSet: FileMessageSet, 
                 val index: OffsetIndex, 
                 val start: Long, 
                 val indexIntervalBytes: Int,
                 time: Time) extends Range {
  
  var firstAppendTime: Option[Long] = None
  
  /* the number of bytes since we last added an entry in the offset index */
  var bytesSinceLastIndexEntry = 0
  
  @volatile var deleted = false
  
  def this(dir: File, startOffset: Long, mutable: Boolean, indexIntervalBytes: Int, maxIndexSize: Int) = 
    this(new FileMessageSet(file = Log.logFilename(dir, startOffset), mutable = mutable), 
         new OffsetIndex(file = Log.indexFilename(dir, startOffset), baseOffset = startOffset, mutable = mutable, maxIndexSize = maxIndexSize),
         startOffset,
         indexIntervalBytes,
         SystemTime)
    
  /* Return the size in bytes of this log segment */
  def size: Long = messageSet.sizeInBytes()

  def updateFirstAppendTime() {
    if (firstAppendTime.isEmpty)
      firstAppendTime = Some(time.milliseconds)
  }

  /**
   * Append the given messages starting with the given offset. Add
   * an entry to the index if needed.
   * 
   * It is assumed this method is being called from within a lock
   */
  def append(offset: Long, messages: ByteBufferMessageSet) {
    if (messages.sizeInBytes > 0) {
      // append an entry to the index (if needed)
      if(bytesSinceLastIndexEntry > indexIntervalBytes) {
        index.append(offset, messageSet.sizeInBytes().toInt)
        this.bytesSinceLastIndexEntry = 0
      }
      // append the messages
      messageSet.append(messages)
      updateFirstAppendTime()
      this.bytesSinceLastIndexEntry += messages.sizeInBytes.toInt
    }
  }
  
  /**
   * Find the physical file position for the least offset >= the given offset. If no offset is found
   * that meets this criteria before the end of the log, return null.
   */
  def translateOffset(offset: Long): OffsetPosition = {
    val mapping = index.lookup(offset)
    messageSet.searchFor(offset, mapping.position)
  }
  
  /**
   * Read a message set from this segment beginning with the first offset
   * greater than or equal to the startOffset. The message set will include
   * no more than maxSize bytes and will end before maxOffset if a maxOffset is specified.
   */
  def read(startOffset: Long, maxSize: Int, maxOffset: Option[Long]): MessageSet = {
    if(maxSize <= 0)
      return MessageSet.Empty
      
    val startPosition = translateOffset(startOffset)
    
    // if the start position is already off the end of the log, return MessageSet.Empty
    if(startPosition == null)
      return MessageSet.Empty
    
    // calculate the length of the message set to read based on whether or not they gave us a maxOffset
    val length = 
      maxOffset match {
        case None =>
          // no max offset, just use the max size they gave unmolested
          maxSize
        case Some(offset) => {
          // there is a max offset, translate it to a file position and use that to calculate the max read size
          val mapping = translateOffset(offset)
          val endPosition = 
            if(mapping == null)
              messageSet.sizeInBytes().toInt // the max offset is off the end of the log, use the end of the file
            else
              mapping.position
          min(endPosition - startPosition.position, maxSize) 
        }
      }
    messageSet.read(startPosition.position, length)
  }

  override def toString() = "LogSegment(start=" + start + ", size=" + size + ")"

  /**
   * Truncate off all index and log entries with offsets greater than or equal to the current offset. 
   */
  def truncateTo(offset: Long) {
    val mapping = translateOffset(offset)
    if(mapping == null)
      return
    index.truncateTo(offset)  
    messageSet.truncateTo(mapping.position)
  }
  
  /**
   * Calculate the offset that would be used for the next message to be append to this segment.
   * Not that this is expensive.
   */
  def nextOffset(): Long = {
    val ms = read(index.lastOffset, messageSet.sizeInBytes.toInt, None)
    ms.lastOption match {
      case None => start
      case Some(last) => last.nextOffset
    }
  }
  
  /**
   * Flush this log segment to disk
   */
  def flush() {
    messageSet.flush()
    index.flush()
  }
  
  /**
   * Close this log segment
   */
  def close() {
    Utils.swallow(index.close)
    Utils.swallow(messageSet.close)
  }
  
}