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
import kafka.log.remote.RemoteIndexCache.DirName
import kafka.utils.{CoreUtils, Logging, ShutdownableThread}
import org.apache.kafka.common.errors.CorruptRecordException
import org.apache.kafka.common.utils.Utils
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
}

class Entry(val offsetIndex: OffsetIndex, val timeIndex: TimeIndex, val txnIndex: TransactionIndex) {
  private val markedForCleanup = new AtomicBoolean(false)

  def lookupOffset(targetOffset: Long): OffsetPosition = {
    if (markedForCleanup.get()) throw new IllegalStateException("This entry is marked for cleanup")
    else offsetIndex.lookup(targetOffset)
  }

  def lookupTimestamp(timestamp: Long, startingOffset: Long): OffsetPosition = {
    if (markedForCleanup.get()) throw new IllegalStateException("This entry is marked for cleanup")

    val timestampOffset = timeIndex.lookup(timestamp)
    offsetIndex.lookup(math.max(startingOffset, timestampOffset.offset))
  }

  def markForCleanup(): Unit = {
    if (!markedForCleanup.getAndSet(true)) {
      Array(offsetIndex, timeIndex, txnIndex).foreach(x =>
        x.renameTo(new File(CoreUtils.replaceSuffix(x.file.getPath, "", UnifiedLog.DeletedFileSuffix))))
    }
  }

  def cleanup(): Unit = {
    markForCleanup()
    CoreUtils.tryAll(Seq(() => offsetIndex.deleteIfExists(), () => timeIndex.deleteIfExists(), () => txnIndex.deleteIfExists()))
  }

  def close(): Unit = {
    Array(offsetIndex, timeIndex, txnIndex).foreach(index => try {
      index.close()
    } catch {
      case _: Exception => // ignore error.
    })
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

  val cacheDir = new File(logDir, DirName)
  @volatile var closed = false

  val expiredIndexes = new LinkedBlockingQueue[Entry]()
  val lock = new Object()

  val entries: util.Map[RemoteLogSegmentId, Entry] = new java.util.LinkedHashMap[RemoteLogSegmentId, Entry](maxSize / 2,
    0.75f, true) {
    override def removeEldestEntry(eldest: util.Map.Entry[RemoteLogSegmentId, Entry]): Boolean = {
      if (this.size() > maxSize) {
        val entry = eldest.getValue
        // Mark the entries for cleanup, background thread will clean them later.
        entry.markForCleanup()
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

    // Delete any .deleted files remained from the earlier run of the broker.
    Files.list(cacheDir.toPath).forEach((path: Path) => {
      if (path.endsWith(UnifiedLog.DeletedFileSuffix)) {
        Files.deleteIfExists(path)
      }
    })
  }

  init()

  // Start cleaner thread that will clean the expired entries
  val cleanerThread: ShutdownableThread = new ShutdownableThread("remote-log-index-cleaner") {
    setDaemon(true)

    override def doWork(): Unit = {
      val entry = expiredIndexes.take()
      info(s"Cleaning up index entry $entry")
      try {
        entry.cleanup()
      } catch {
        case ex: Exception => error("Error occurred while fetching/cleaning up expired entry", ex)
      }
    }
  }
  cleanerThread.start()

  def getIndexEntry(remoteLogSegmentMetadata: RemoteLogSegmentMetadata): Entry = {
    def loadIndexFile[T <: CleanableIndex](fileName: String,
                                           suffix: String,
                                           fetchRemoteIndex: RemoteLogSegmentMetadata => InputStream,
                                           readIndex: File => T): T = {
      val indexFile = new File(cacheDir, fileName + suffix)

      def fetchAndCreateIndex(): T = {
        val tmpIndexFile = new File(cacheDir, fileName + suffix + RemoteIndexCache.TmpFileSuffix)

        val inputStream = fetchRemoteIndex(remoteLogSegmentMetadata)
        try {
          Files.copy(inputStream, tmpIndexFile.toPath)
        } finally {
          if (inputStream != null) {
            inputStream.close()
          }
        }

        Utils.atomicMoveWithFallback(tmpIndexFile.toPath, indexFile.toPath, false)
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

        val offsetIndex: OffsetIndex = loadIndexFile(fileName, UnifiedLog.IndexFileSuffix,
          rlsMetadata => remoteStorageManager.fetchIndex(rlsMetadata, IndexType.OFFSET),
          file => {
            val index = new OffsetIndex(file, startOffset, Int.MaxValue, writable = false)
            index.sanityCheck()
            index
          })

        val timeIndex: TimeIndex = loadIndexFile(fileName, UnifiedLog.TimeIndexFileSuffix,
          rlsMetadata => remoteStorageManager.fetchIndex(rlsMetadata, IndexType.TIMESTAMP),
          file => {
            val index = new TimeIndex(file, startOffset, Int.MaxValue, writable = false)
            index.sanityCheck()
            index
          })

        val txnIndex: TransactionIndex = loadIndexFile(fileName, UnifiedLog.TxnIndexFileSuffix,
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

  def close(): Unit = {
    closed = true
    cleanerThread.shutdown()
    // Close all the opened indexes.
    lock synchronized {
      entries.values().stream().forEach(entry => entry.close())
    }
  }

}
