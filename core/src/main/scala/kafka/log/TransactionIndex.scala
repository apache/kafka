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

import java.io.{File, IOException}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption

import kafka.utils.{Logging, nonthreadsafe}
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.requests.FetchResponse.AbortedTransaction
import org.apache.kafka.common.utils.Utils

import scala.collection.mutable.ListBuffer

private[log] case class TxnIndexSearchResult(abortedTransactions: List[AbortedTxn], isComplete: Boolean)

/**
 * The transaction index maintains metadata about the aborted transactions for each segment. This includes
 * the start and end offsets for the aborted transactions and the last stable offset (LSO) at the time of
 * the abort. This index is used to find the aborted transactions in the range of a given fetch request at
 * the READ_COMMITTED isolation level.
 *
 * There is at most one transaction index for each log segment. The entries correspond to the transactions
 * whose commit markers were written in the corresponding log segment. Note, however, that individual transactions
 * may span multiple segments. Recovering the index therefore requires scanning the earlier segments in
 * order to find the start of the transactions.
 */
@nonthreadsafe
class TransactionIndex(val startOffset: Long, @volatile var file: File) extends Logging {
  // note that the file is not created until we need it
  @volatile private var maybeChannel: Option[FileChannel] = None
  private var lastOffset: Option[Long] = None

  if (file.exists)
    openChannel()

  def append(abortedTxn: AbortedTxn): Unit = {
    lastOffset.foreach { offset =>
      if (offset >= abortedTxn.lastOffset)
        throw new IllegalArgumentException("The last offset of appended transactions must increase sequentially")
    }
    lastOffset = Some(abortedTxn.lastOffset)
    Utils.writeFully(channel, abortedTxn.buffer.duplicate())
  }

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

  def truncate() = {
    maybeChannel.foreach(_.truncate(0))
    lastOffset = None
  }

  def close(): Unit = {
    maybeChannel.foreach(_.close())
    maybeChannel = None
  }

  def renameTo(f: File): Unit = {
    try {
      if (file.exists)
        Utils.atomicMoveWithFallback(file.toPath, f.toPath)
    } finally file = f
  }

  def truncateTo(offset: Long): Unit = {
    val buffer = ByteBuffer.allocate(AbortedTxn.TotalSize)
    var newLastOffset: Option[Long] = None
    for ((abortedTxn, position) <- iterator(() => buffer)) {
      if (abortedTxn.lastOffset >= offset) {
        channel.truncate(position)
        lastOffset = newLastOffset
        return
      }
      newLastOffset = Some(abortedTxn.lastOffset)
    }
  }

  private def iterator(allocate: () => ByteBuffer = () => ByteBuffer.allocate(AbortedTxn.TotalSize)): Iterator[(AbortedTxn, Int)] = {
    maybeChannel match {
      case None => Iterator.empty
      case Some(channel) =>
        var position = 0

        new Iterator[(AbortedTxn, Int)] {
          override def hasNext: Boolean = channel.position - position >= AbortedTxn.TotalSize

          override def next(): (AbortedTxn, Int) = {
            try {
              val buffer = allocate()
              Utils.readFully(channel, buffer, position)
              buffer.flip()

              val abortedTxn = new AbortedTxn(buffer)
              if (abortedTxn.version > AbortedTxn.CurrentVersion)
                throw new KafkaException(s"Unexpected aborted transaction version ${abortedTxn.version}, " +
                  s"current version is ${AbortedTxn.CurrentVersion}")
              val nextEntry = (abortedTxn, position)
              position += AbortedTxn.TotalSize
              nextEntry
            } catch {
              case e: IOException =>
                // We received an unexpected error reading from the index file. We propagate this as an
                // UNKNOWN error to the consumer, which will cause it to retry the fetch.
                throw new KafkaException(s"Failed to read from the transaction index $file", e)
            }
          }
        }
    }
  }

  def allAbortedTxns: List[AbortedTxn] = {
    iterator().map(_._1).toList
  }

  /**
   * Collect all aborted transactions which overlap with a given fetch range.
   *
   * @param fetchOffset Inclusive first offset of the fetch range
   * @param upperBoundOffset Exclusive last offset in the fetch range
   * @return An object containing the aborted transactions and whether the search needs to continue
   *         into the next log segment.
   */
  def collectAbortedTxns(fetchOffset: Long, upperBoundOffset: Long): TxnIndexSearchResult = {
    val abortedTransactions = ListBuffer.empty[AbortedTxn]
    for ((abortedTxn, _) <- iterator()) {
      if (abortedTxn.lastOffset >= fetchOffset && abortedTxn.firstOffset < upperBoundOffset)
        abortedTransactions += abortedTxn

      if (abortedTxn.lastStableOffset >= upperBoundOffset)
        return TxnIndexSearchResult(abortedTransactions.toList, isComplete = true)
    }
    TxnIndexSearchResult(abortedTransactions.toList, isComplete = false)
  }

  def sanityCheck(): Unit = {
    val buffer = ByteBuffer.allocate(AbortedTxn.TotalSize)
    for ((abortedTxn, _) <- iterator(() => buffer)) {
      require(abortedTxn.lastOffset >= startOffset, s"Last offset of aborted transaction $abortedTxn is less than " +
        s"start offset $startOffset")
    }
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
    this(completedTxn.producerId, completedTxn.firstOffset, completedTxn.lastOffset, lastStableOffset)

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
