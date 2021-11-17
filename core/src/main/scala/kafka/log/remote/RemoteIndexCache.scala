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

import kafka.log._
import kafka.utils.{CoreUtils, Logging}
import org.apache.kafka.common.errors.CorruptRecordException
import org.apache.kafka.common.utils.{KafkaThread, Utils}
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType
import org.apache.kafka.server.log.remote.storage.{RemoteLogSegmentId, RemoteLogSegmentMetadata, RemoteStorageManager}

import java.io.{File, InputStream}
import java.nio.file.{Files, Path}
import java.util
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicBoolean

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

  def lookupTimestamp(timestamp: Long, startingOffset: Long): OffsetPosition = {
    if (closed.get()) throw new IllegalStateException("This entry is already closed")

    val timestampOffset = timeIndex.lookup(timestamp)
    offsetIndex.lookup(math.max(startingOffset, timestampOffset.offset))
  }

  def close(): Unit = {
    if (!closed.getAndSet(true)) {
      Array(offsetIndex, timeIndex, txnIndex).foreach(x =>
        x.renameTo(new File(CoreUtils.replaceSuffix(x.file.getPath, "", UnifiedLog.DeletedFileSuffix))))
    }
  }

  def cleanup(): Unit = {
    close()
    CoreUtils.tryAll(Seq(() => offsetIndex.deleteIfExists(), () => timeIndex.deleteIfExists(), () => txnIndex.deleteIfExists()))
  }
}


/**
 * This is a LRU cache of remote index files stored in `$logdir/remote-log-index-cache`. This is helpful to avoid
 * re-fetching the index files like offset, time indexes from the remote storage for every fetch call.
 *
 * @param maxSize
 * @param remoteStorageManager
 * @param logDir
 */
//todo-tier make maxSize configurable
class RemoteIndexCache(maxSize: Int = 1024, remoteStorageManager: RemoteStorageManager, logDir: String) extends Logging {

  val cacheDir = new File(logDir, "remote-log-index-cache")
  @volatile var closed = false

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

  private def init(): Unit = {
    if (cacheDir.mkdir())
      info(s"Created $cacheDir successfully")

    // delete any .deleted files remained from the earlier run of the broker.
    Files.list(cacheDir.toPath).forEach((path: Path) => {
      if (path.endsWith(UnifiedLog.DeletedFileSuffix)) {
        Files.deleteIfExists(path)
      }
    })
  }

  init()

  // Start cleaner thread that will clean the expired entries
  val cleanerThread: KafkaThread = KafkaThread.daemon("remote-log-index-cleaner", () => {
    while (!closed) {
      try {
        val entry = expiredIndexes.take()
        info(s"Cleaning up index entry $entry")
        entry.cleanup()
      } catch {
        case ex: Exception => error("Error occurred while fetching/cleaning up expired entry", ex)
      }
    }
  })
  cleanerThread.start()

  def getIndexEntry(remoteLogSegmentMetadata: RemoteLogSegmentMetadata): Entry = {
    def loadIndexFile[T <: CleanableIndex](fileName: String,
                                           suffix: String,
                                           fetchRemoteIndex: RemoteLogSegmentMetadata => InputStream,
                                           readIndex: File => T): T = {
      val indexFile = new File(cacheDir, fileName + suffix)

      def fetchAndCreateIndex(): T = {
        val inputStream = fetchRemoteIndex(remoteLogSegmentMetadata)
        val tmpIndexFile = new File(cacheDir, fileName + suffix + RemoteIndexCache.TmpFileSuffix)

        Files.copy(inputStream, tmpIndexFile.toPath)

        Utils.atomicMoveWithFallback(tmpIndexFile.toPath, indexFile.toPath)
        readIndex(indexFile)
      }

      if (indexFile.exists()) {
        try {
          readIndex(indexFile)
        } catch {
          case ex: CorruptRecordException =>
            info("Error occurred while loading the stored index", ex)
            fetchAndCreateIndex()
        }
      } else {
        fetchAndCreateIndex()
      }
    }

    lock synchronized {
      entries.computeIfAbsent(remoteLogSegmentMetadata.remoteLogSegmentId(), (key: RemoteLogSegmentId) => {
        val fileName = key.id().toString
        val startOffset = remoteLogSegmentMetadata.startOffset()

        val offsetIndex: OffsetIndex = loadIndexFile(fileName, RemoteIndexCache.OffsetIndexFileSuffix,
          rlsMetadata => remoteStorageManager.fetchIndex(rlsMetadata, IndexType.OFFSET),
          file => {
            val index = new OffsetIndex(file, startOffset, Int.MaxValue, writable = false)
            index.sanityCheck()
            index
          })

        val timeIndex: TimeIndex = loadIndexFile(fileName, RemoteIndexCache.TimeIndexFileSuffix,
          rlsMetadata => remoteStorageManager.fetchIndex(rlsMetadata, IndexType.TIMESTAMP),
          file => {
            val index = new TimeIndex(file, startOffset, Int.MaxValue, writable = false)
            index.sanityCheck()
            index
          })

        val txnIndex: TransactionIndex = loadIndexFile(fileName, RemoteIndexCache.TxnIndexFileSuffix,
          rlsMetadata => remoteStorageManager.fetchIndex(rlsMetadata, IndexType.TRANSACTION),
          file => {
            val index = new TransactionIndex(startOffset, file)
            index.sanityCheck()
            index
          })

        new Entry(offsetIndex, timeIndex, txnIndex)
      })
    }
  }

  def lookupOffset(remoteLogSegmentMetadata: RemoteLogSegmentMetadata, offset: Long): Int = {
    getIndexEntry(remoteLogSegmentMetadata).lookupOffset(offset).position
  }

  def lookupTimestamp(remoteLogSegmentMetadata: RemoteLogSegmentMetadata, timestamp: Long, startingOffset: Long): Int = {
    getIndexEntry(remoteLogSegmentMetadata).lookupTimestamp(timestamp, startingOffset).position
  }

  def collectAbortedTransaction(remoteLogSegmentMetadata: RemoteLogSegmentMetadata,
                                startOffset: Long,
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
