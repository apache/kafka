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

import java.io.{Closeable, File, IOException}
import java.nio.channels.FileChannel
import java.nio.file.{Files, StandardOpenOption}

import kafka.utils.Logging
import org.apache.kafka.common.utils.Utils

/**
 * The remote log index maintains the information of log records for each topic partition maintained in the remote log.
 * It is a sequence of remote log entries.
 *
 * Each remote log entry is represented with [[RemoteLogIndexEntry]]
 *
 * todo-satish: currently it is using a channel to store the entries. We may go with memory mapped file later to
 * read/write the entries. Need to add methods to fetch remote log offset for a given offset/timestamp.
 *
 */
class RemoteLogIndex(val startOffset: Long, @volatile var file: File) extends Logging with Closeable {

  @volatile private var maybeChannel: Option[FileChannel] = None
  private var lastOffset: Option[Long] = None

  if (file.exists)
    openChannel()

  def append(entry: RemoteLogIndexEntry): Unit = {
    lastOffset.foreach { offset =>
      if (offset >= entry.lastOffset)
        throw new IllegalArgumentException(s"The last offset of appended log entry must increase sequentially, but " +
          s"${entry.lastOffset} is not greater than current last offset $offset of index ${file.getAbsolutePath}")
    }
    lastOffset = Some(entry.lastOffset)
    Utils.writeFully(channel(), entry.asBuffer.duplicate())
  }

  def flush(): Unit = maybeChannel.foreach(_.force(true))

  /**
   * Delete this index.
   *
   * @throws IOException if deletion fails due to an I/O error
   * @return `true` if the file was deleted by this method; `false` if the file could not be deleted because it did
   *         not exist
   */
  def deleteIfExists(): Boolean = {
    close()
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
    lastOffset = None
  }

  def close(): Unit = {
    maybeChannel.foreach(_.close())
    maybeChannel = None
  }

}
