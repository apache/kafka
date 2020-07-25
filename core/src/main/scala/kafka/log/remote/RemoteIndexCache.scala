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

import java.io.{File, InputStream}
import java.nio.file.{Files, Path}
import java.util
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicBoolean
import java.util.function.{Consumer, Function}

import kafka.log.{CleanableIndex, Log, OffsetIndex, OffsetPosition, TimeIndex, TransactionIndex, TxnIndexSearchResult}
import kafka.utils.{CoreUtils, Logging}
import org.apache.kafka.common.errors.CorruptRecordException
import org.apache.kafka.common.log.remote.storage.{RemoteLogSegmentId, RemoteLogSegmentMetadata, RemoteStorageManager}
import org.apache.kafka.common.utils.{KafkaThread, Utils}

object RemoteIndexCache {
  val DirName = "remote-log-index-cache"
  val TmpFileSuffix = ".tmp"
  val OffsetIndexFileSuffix = ".oi"
  val TimeIndexFileSuffix = ".ti"
  val TxnIndexFileSuffix = ".tx"
}

class Entry(val offsetIndex: OffsetIndex, val timeIndex: TimeIndex, val txnIndex: TransactionIndex) {
  private val closed = new AtomicBoolean(false)

  def lookupOffset(targetOffset: Long): OffsetPosition = {
    if (closed.get()) throw new IllegalStateException("This entry is already closed")
    else offsetIndex.lookup(targetOffset)
  }

  def lookupTimestamp(timestamp: Long, startingOffset: Long): Long = {
    if (closed.get()) throw new IllegalStateException("This entry is already closed")

    val timestampOffset = timeIndex.lookup(timestamp)
    offsetIndex.lookup(math.max(startingOffset, timestampOffset.offset)).position
  }

  def close(): Unit = {
    if (!closed.getAndSet(true)) {
      Array(offsetIndex, timeIndex, txnIndex).foreach(x =>
        x.renameTo(new File(CoreUtils.replaceSuffix(x.file.getPath, "", Log.DeletedFileSuffix))))
    }
  }

  def cleanup(): Unit = {
    close()
    CoreUtils.tryAll(Seq(() => offsetIndex.deleteIfExists(), () => timeIndex.deleteIfExists(), () => txnIndex.deleteIfExists()))
  }
}

//todo-tier make maxSize configurable
class RemoteIndexCache(maxSize: Int = 1024, remoteStorageManager: RemoteStorageManager, logDir: String) extends Logging {

  val cacheDir = new File(logDir, "remote-log-index-cache")
  @volatile var closed = false;

  val expiredIndexes = new LinkedBlockingQueue[Entry]()
  val lock = new Object()

  val entries: util.Map[RemoteLogSegmentId, Entry] = new java.util.LinkedHashMap[RemoteLogSegmentId, Entry](maxSize / 2,
    0.75f, true) {
    override def removeEldestEntry(eldest: util.Map.Entry[RemoteLogSegmentId, Entry]): Boolean = {
      if (this.size() >= maxSize) {
        val entry = eldest.getValue
        // close the entries, background thread will clean them later.
        entry.close()
        expiredIndexes.add(entry)
        true
      } else {
        false
      }
    }
  }

  private def init() = {
    if (cacheDir.mkdir()) info(s"Created $cacheDir successfully")

    // delete any .deleted files remained from the earlier run of the broker.
    Files.list(cacheDir.toPath).forEach(new Consumer[Path] {
      override def accept(path: Path): Unit = {
        if (path.endsWith(Log.DeletedFileSuffix) || path.endsWith(Log.DeletedFileSuffix)) {
          Files.deleteIfExists(path)
        }
      }
    })

    //todo-tier load the stored entries into the cache.
  }

  init()

  // Start cleaner thread that will clean the expired entries
  val cleanerThread = KafkaThread.daemon("remote-log-index-cleaner", new Runnable {
    override def run(): Unit = {
      while (!closed) {
        try {
          val entry = expiredIndexes.take()
          info(s"Cleaning up index entry $entry")
          entry.cleanup()
        } catch {
          case ex: Exception => error("Error occurred while fetching/cleaning up expired entry", ex)
        }
      }
    }
  })
  cleanerThread.start()

  def getIndexEntry(remoteLogSegmentMetadata: RemoteLogSegmentMetadata): Entry = {
    def loadIndexFile[T <: CleanableIndex](fileName: String, suffix: String,
                                                fetchRemoteIndex: RemoteLogSegmentMetadata => InputStream,
                                                createIndex: File => T): T = {
      val indexFile = new File(cacheDir, fileName + suffix)

      def fetchAndCreateIndex(): T = {
        val inputStream = fetchRemoteIndex(remoteLogSegmentMetadata)
        val tmpIndexFile = new File(cacheDir, fileName + suffix + RemoteIndexCache.TmpFileSuffix)

        // Below FileChannel#transferFrom call may be efficient as it goes through a fast path of transferring directly
        //from the source channel into the filesystem cache. But if it goes through non-fast path then it expects the
        //inputStream to always have available bytes. This is an unnecessary restriction on RemoteStorageManager to
        //always return InputStream to have available bytes till the end.
        // FileChannel.open(tmpIndexFile.toPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE)
        // .transferFrom(sourceChannel, 0, Int.MaxValue)
        Files.copy(inputStream, tmpIndexFile.toPath)

        Utils.atomicMoveWithFallback(tmpIndexFile.toPath, indexFile.toPath)
        createIndex(indexFile)
      }

      if (indexFile.exists()) {
        try {
          createIndex(indexFile)
        } catch {
          case ex: CorruptRecordException =>
            info("Error occurred while loading the stored index ", ex)
            fetchAndCreateIndex()
        }
      } else {
        fetchAndCreateIndex()
      }
    }

    lock synchronized {
      entries.computeIfAbsent(remoteLogSegmentMetadata.remoteLogSegmentId(), new Function[RemoteLogSegmentId, Entry] {
        override def apply(key: RemoteLogSegmentId): Entry = {
          val fileSuffix = remoteLogSegmentMetadata.remoteLogSegmentId().id().toString
          val startOffset = remoteLogSegmentMetadata.startOffset()

          val offsetIndex: OffsetIndex = loadIndexFile(fileSuffix, RemoteIndexCache.OffsetIndexFileSuffix,
            rlsMetadata => remoteStorageManager.fetchOffsetIndex(rlsMetadata), file => {
              val index = new OffsetIndex(file, startOffset, Int.MaxValue, writable = false)
              index.sanityCheck()
              index
            })

          val timeIndex = loadIndexFile(fileSuffix, RemoteIndexCache.TimeIndexFileSuffix,
            rlsMetadata => remoteStorageManager.fetchTimestampIndex(rlsMetadata),
            file => {
              val index = new TimeIndex(file, startOffset, Int.MaxValue, writable = false)
              index.sanityCheck()
              index
            })

          val txnIndex:TransactionIndex = loadIndexFile(fileSuffix, RemoteIndexCache.TxnIndexFileSuffix,
            rlsMetadata => remoteStorageManager.fetchTransactionIndex(rlsMetadata),
            file => {
              val index = new TransactionIndex(startOffset, file)
              index.sanityCheck()
              index
            })

          new Entry(offsetIndex, timeIndex, txnIndex)
        }
      })
    }
  }

  def lookupOffset(remoteLogSegmentMetadata: RemoteLogSegmentMetadata, offset: Long): Long = {
    getIndexEntry(remoteLogSegmentMetadata).lookupOffset(offset).position
  }

  def lookupTimestamp(remoteLogSegmentMetadata: RemoteLogSegmentMetadata, timestamp: Long, startingOffset: Long): Long = {
    getIndexEntry(remoteLogSegmentMetadata).lookupTimestamp(timestamp, startingOffset)
  }

  def collectAbortedTransaction(remoteLogSegmentMetadata: RemoteLogSegmentMetadata, startOffset: Long,
                                fetchSize: Int): TxnIndexSearchResult = {
    val entry = getIndexEntry(remoteLogSegmentMetadata)
    val maxOffset = entry.offsetIndex.fetchUpperBoundOffset(entry.offsetIndex.lookup(startOffset), fetchSize)
      .map(_.offset)

    maxOffset.map(x => entry.txnIndex.collectAbortedTxns(startOffset, x))
      .getOrElse(TxnIndexSearchResult(List.empty, isComplete = false))
  }

  def close(): Unit = {
    closed = true
    cleanerThread.interrupt()
  }

}
