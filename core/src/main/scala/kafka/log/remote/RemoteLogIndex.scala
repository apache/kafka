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
package kafka.log.remote

import java.io.{File, IOException}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.{Files, StandardOpenOption}
import java.util.concurrent.locks.ReentrantLock

import kafka.log.CleanableIndex
import kafka.utils.CoreUtils.inLock
import kafka.utils.Logging
import org.apache.kafka.common.log.remote.storage.RemoteLogIndexEntry
import org.apache.kafka.common.utils.Utils

/**
 * The remote log index maintains the information of log records for each topic partition maintained in the remote log.
 * It is a sequence of remote log entries.
 *
 * Each remote log entry is represented with [[RemoteLogIndexEntry]]
 *
 */
class RemoteLogIndex(_file: File, val startOffset: Long) extends CleanableIndex(_file) with Logging {

  @volatile private var maybeChannel: Option[FileChannel] = None

  protected val lock = new ReentrantLock

  if (file.exists)
    openChannel()

  private def loadLastOffset(): Option[Long] = {
    val curPos = channel().position()
    if (curPos > 0) {
      channel().position(curPos - 8)
      val buffer = ByteBuffer.allocate(8)
      channel().read(buffer)
      buffer.flip()
      val lastEntryPos = buffer.getLong()
      lookupEntry(lastEntryPos).map(x => x.lastOffset)
    } else None
  }

  def lastOffset: Option[Long] = {
    _lastOffset
  }

  private var _lastOffset: Option[Long] = {
    inLock(lock) {
      loadLastOffset()
    }
  }

  private[remote] def append(entries: Seq[RemoteLogIndexEntry]): Seq[Long] = {
    inLock(lock) {
      val positions: Seq[Long] = entries.map(entry => append(entry))
      flush()
      positions
    }
  }

  /**
   * Appends the given entry and returns the position of that entry on the index.
   *
   * @param entry
   * @return the position of the added entry into this index.
   */
  private def append(entry: RemoteLogIndexEntry): Long = {
    inLock(lock) {
      _lastOffset.foreach { offset =>
        if (offset >= entry.lastOffset)
          throw new IllegalArgumentException(s"The last offset of appended log entry must increase sequentially, but " +
            s"${entry.lastOffset} is not greater than current last offset $offset of index ${file.getAbsolutePath}")
      }

      def maybeResetPosition(): Long = {
        val curPos = channel().position()
        if (curPos > 0) {
          channel().position(curPos - 8)
        }
        channel().position()
      }

      val entryPos: Long = maybeResetPosition()
      Utils.writeFully(channel(), entry.asBuffer)

      //todo this is a temporary change, will have a better way
      //For now, writing the last entry position to read the last entry from the end
      //it has issues like incomplete copies incase of non-graceful broker shutdown etc may create issues while reloading
      //this will be enhanced like other local index files to handle different scenarios
      val buffer: ByteBuffer = ByteBuffer.allocate(8).putLong(entryPos)
      buffer.flip()
      Utils.writeFully(channel(), buffer)

      _lastOffset = Some(entry.lastOffset)

      entryPos
    }
  }

  def flush(): Unit = maybeChannel.foreach(_.force(true))

  def lookupEntry(position: Long): Option[RemoteLogIndexEntry] = {
    inLock(lock) {
      val value = RemoteLogIndexEntry.parseEntry(channel(), position)
      if (value.isPresent) Some(value.get()) else None
    }
  }

  def nextEntryPosition(): Long = {
    inLock(lock) {
      val position = channel().position()
      if (position > 0) position - 8 else position
    }
  }

  /**
   * Delete this index.
   *
   * @throws IOException if deletion fails due to an I/O error
   * @return `true` if the file was deleted by this method; `false` if the file could not be deleted because it did
   *         not exist
   */
  def deleteIfExists(): Boolean = {
    try {
      close()
    } catch {
      case ex: Exception => debug(s"Exception occurred while closing file:$file", ex)
    }
    Files.deleteIfExists(file.toPath)
  }

  private def channel(): FileChannel = {
    maybeChannel match {
      case Some(channel) => channel
      case None => openChannel()
    }
  }

  private def openChannel(): FileChannel = {
    val channel = FileChannel.open(file.toPath, StandardOpenOption.CREATE, StandardOpenOption.READ,
      StandardOpenOption.WRITE)
    maybeChannel = Some(channel)
    channel.position(channel.size)
    channel
  }

  /**
   */
  def reset(): Unit = {
    maybeChannel.foreach(_.truncate(0))
    _lastOffset = None
  }

  def close(): Unit = {
    maybeChannel.foreach(_.close())
    maybeChannel = None
  }

}

object RemoteLogIndex {
  val SUFFIX = ".remoteLogIndex"

  def fileNamePrefix(fileName: String): String = {
    if (!fileName.endsWith(SUFFIX)) throw new IllegalArgumentException("file name should end with " + SUFFIX)
    fileName.substring(0, fileName.lastIndexOf('.'))
  }

  def open(file: File): RemoteLogIndex = {
    new RemoteLogIndex(file, fileNamePrefix(file.getName).toLong)
  }
}
