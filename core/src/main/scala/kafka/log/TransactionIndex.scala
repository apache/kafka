/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import java.io.File
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption
import java.util.concurrent.locks.ReentrantLock

import kafka.utils.CoreUtils.inLock
import kafka.utils.Logging
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.requests.FetchResponse.AbortedTransaction
import org.apache.kafka.common.utils.Utils

import scala.collection.mutable.ListBuffer

private[log] case class TxnIndexSearchResult(abortedTransactions: List[AbortedTransaction], isComplete: Boolean)

class TransactionIndex(var file: File) extends Logging {
  // note that the file is not created until we need it
  private var maybeChannel: Option[FileChannel] = None
  private val lock = new ReentrantLock

  if (file.exists)
    openChannel()

  def append(abortedTxn: AbortedTxn): Unit = Utils.writeFully(channel, abortedTxn.buffer.duplicate())

  def flush(): Unit = maybeChannel.foreach(_.force(true))

  def delete(): Boolean = {
    maybeChannel.forall { channel =>
      channel.force(true)
      close()
      file.delete()
    }
  }

  private def channel: FileChannel = {
    maybeChannel match {
      case Some(channel) => channel
      case None => openChannel()
    }
  }

  private def openChannel(): FileChannel = {
    val channel = FileChannel.open(file.toPath, StandardOpenOption.READ, StandardOpenOption.WRITE,
      StandardOpenOption.CREATE)
    maybeChannel = Some(channel)
    channel.position(channel.size)
    channel
  }

  def truncate() = maybeChannel.foreach(_.truncate(0))

  def close() {
    maybeChannel.foreach(_.close())
    maybeChannel = None
  }

  def renameTo(f: File) {
    try {
      if (file.exists)
        Utils.atomicMoveWithFallback(file.toPath, f.toPath)
    } finally file = f
  }

  def truncateTo(offset: Long): Unit = {
    maybeChannel.foreach { channel =>
      inLock(lock) {
        var position = 0
        val lastPosition = channel.position()
        while (lastPosition - position >= AbortedTxn.TotalSize) {
          val buffer = ByteBuffer.allocate(AbortedTxn.TotalSize)
          Utils.readFully(channel, buffer, position)
          buffer.flip()

          val abortedTxn = new AbortedTxn(buffer)
          if (abortedTxn.version > AbortedTxn.CurrentVersion)
            trace(s"Reading aborted txn entry with version ${abortedTxn.version} as current " +
              s"version ${AbortedTxn.CurrentVersion}")

          if (abortedTxn.lastOffset >= offset) {
            channel.truncate(position)
            return
          }
          position += AbortedTxn.TotalSize
        }
      }
    }
  }

  def iterator: Iterator[AbortedTxn] = {
    maybeChannel match {
      case None => Iterator.empty
      case Some(channel) =>
        val lastPosition = channel.position()
        var position = 0

        new Iterator[AbortedTxn] {
          override def hasNext: Boolean = lastPosition - position >= AbortedTxn.TotalSize

          override def next(): AbortedTxn = {
            val buffer = ByteBuffer.allocate(AbortedTxn.TotalSize)
            Utils.readFully(channel, buffer, position)
            buffer.flip()

            val abortedTxn = new AbortedTxn(buffer)
            if (abortedTxn.version > AbortedTxn.CurrentVersion)
              throw new KafkaException(s"Unexpected aborted transaction version ${abortedTxn.version}, " +
                s"current version is ${AbortedTxn.CurrentVersion}")
            position += AbortedTxn.TotalSize
            abortedTxn
          }
        }
    }
  }

  def collectAbortedTxns(fetchOffset: Long, upperBoundOffset: Long): TxnIndexSearchResult = {
    val abortedTransactions = ListBuffer.empty[AbortedTransaction]
    for (abortedTxn <- iterator) {
      if (abortedTxn.lastOffset >= fetchOffset && abortedTxn.firstOffset < upperBoundOffset)
        abortedTransactions += abortedTxn.asAbortedTransaction

      if (abortedTxn.lastStableOffset >= upperBoundOffset)
        return TxnIndexSearchResult(abortedTransactions.toList, isComplete = true)
    }
    TxnIndexSearchResult(abortedTransactions.toList, isComplete = false)
  }
}

private[log] object AbortedTxn {
  val VersionOffset = 0
  val VersionSize = 2
  val ProducerIdOffset = VersionOffset + VersionSize
  val ProducerIdSize = 8
  val FirstOffsetOffset = ProducerIdOffset + ProducerIdSize
  val FirstOffsetSize = 8
  val LastOffsetOffset = FirstOffsetOffset + FirstOffsetSize
  val LastOffsetSize = 8
  val LastStableOffsetOffset = LastOffsetOffset + LastOffsetSize
  val LastStableOffsetSize = 8
  val TotalSize = LastStableOffsetOffset + LastStableOffsetSize

  val CurrentVersion: Short = 0
}

private[log] class AbortedTxn(val buffer: ByteBuffer) {
  import AbortedTxn._

  def this(producerId: Long,
           firstOffset: Long,
           lastOffset: Long,
           lastStableOffset: Long) = {
    this(ByteBuffer.allocate(AbortedTxn.TotalSize))
    buffer.putShort(CurrentVersion)
    buffer.putLong(producerId)
    buffer.putLong(firstOffset)
    buffer.putLong(lastOffset)
    buffer.putLong(lastStableOffset)
    buffer.flip()
  }

  def this(completedTxn: CompletedTxn, lastStableOffset: Long) =
    this(completedTxn.pid, completedTxn.firstOffset, completedTxn.lastOffset, lastStableOffset)

  def version: Short = buffer.get(VersionOffset)

  def producerId: Long = buffer.getLong(ProducerIdOffset)

  def firstOffset: Long = buffer.getLong(FirstOffsetOffset)

  def lastOffset: Long = buffer.getLong(LastOffsetOffset)

  def lastStableOffset: Long = buffer.getLong(LastStableOffsetOffset)

  def asAbortedTransaction: AbortedTransaction = new AbortedTransaction(producerId, firstOffset)

  override def toString: String =
    s"AbortedTxn(version=$version, producerId=$producerId, firstOffset=$firstOffset, " +
      s"lastOffset=$lastOffset, lastStableOffset=$lastStableOffset)"

  override def equals(any: Any): Boolean = {
    any match {
      case that: AbortedTxn => this.buffer.equals(that.buffer)
      case _ => false
    }
  }

  override def hashCode(): Int = buffer.hashCode
}
