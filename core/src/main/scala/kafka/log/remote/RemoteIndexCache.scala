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

import com.github.benmanes.caffeine.cache.{Cache, Caffeine, RemovalCause}
import kafka.log.UnifiedLog
import kafka.log.remote.RemoteIndexCache.{DirName, remoteLogIndexCacheCleanerThread}
import kafka.utils.CoreUtils.{inReadLock, inWriteLock}
import kafka.utils.{CoreUtils, Logging, threadsafe}
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.errors.CorruptRecordException
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType
import org.apache.kafka.server.log.remote.storage.{RemoteLogSegmentMetadata, RemoteStorageManager}
import org.apache.kafka.storage.internals.log.{LogFileUtils, OffsetIndex, OffsetPosition, TimeIndex, TransactionIndex}
import org.apache.kafka.server.util.ShutdownableThread

import java.io.{File, InputStream}
import java.nio.file.{FileAlreadyExistsException, Files, Path}
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantReadWriteLock

object RemoteIndexCache {
  val DirName = "remote-log-index-cache"
  val TmpFileSuffix = ".tmp"
  val remoteLogIndexCacheCleanerThread = "remote-log-index-cleaner"
}

@threadsafe
class Entry(val offsetIndex: OffsetIndex, val timeIndex: TimeIndex, val txnIndex: TransactionIndex) extends AutoCloseable {
  // visible for testing
  private[remote] var markedForCleanup = false
  // visible for testing
  private[remote] var cleanStarted = false
  // This lock is used to synchronize cleanup methods and read methods. This ensures that cleanup (which changes the
  // underlying files of the index) isn't performed while a read is in-progress for the entry. This is required in
  // addition to using the thread safe cache because, while the thread safety of the cache ensures that we can read
  // entries concurrently, it does not ensure that we won't mutate underlying files beloging to an entry.
  private val lock: ReentrantReadWriteLock = new ReentrantReadWriteLock()

  def lookupOffset(targetOffset: Long): OffsetPosition = {
    inReadLock(lock) {
      if (markedForCleanup) throw new IllegalStateException("This entry is marked for cleanup")
      offsetIndex.lookup(targetOffset)
    }
  }

  def lookupTimestamp(timestamp: Long, startingOffset: Long): OffsetPosition = {
    inReadLock(lock) {
      if (markedForCleanup) throw new IllegalStateException("This entry is marked for cleanup")
      val timestampOffset = timeIndex.lookup(timestamp)
      offsetIndex.lookup(math.max(startingOffset, timestampOffset.offset))
    }
  }

  private[remote] def markForCleanup(): Unit = {
    inWriteLock(lock) {
      if (!markedForCleanup) {
        markedForCleanup = true
        Array(offsetIndex, timeIndex).foreach(index =>
          index.renameTo(new File(Utils.replaceSuffix(index.file.getPath, "", LogFileUtils.DELETED_FILE_SUFFIX))))
        // txn index needs to be renamed separately since it's not of type AbstractIndex
        txnIndex.renameTo(new File(Utils.replaceSuffix(txnIndex.file.getPath, "",
          LogFileUtils.DELETED_FILE_SUFFIX)))
      }
    }
  }

  /**
   * Deletes the index files from the disk. Invoking #close is not required prior to this function.
   */
  private[remote] def cleanup(): Unit = {
    inWriteLock(lock) {
      markForCleanup()
      // no-op if clean is done already
      if (!cleanStarted) {
        cleanStarted = true
        CoreUtils.tryAll(Seq(() => offsetIndex.deleteIfExists(), () => timeIndex.deleteIfExists(), () => txnIndex.deleteIfExists()))
      }
    }
  }

  /**
   * Calls the underlying close method for each index which may lead to releasing resources such as mmap.
   * This function does not delete the index files.
   */
  @Override
  def close(): Unit = {
    inWriteLock(lock) {
      // close is no-op if entry is already marked for cleanup. Mmap resources are released during cleanup.
      if (!markedForCleanup) {
        Utils.closeQuietly(offsetIndex, "Closing the offset index.")
        Utils.closeQuietly(timeIndex, "Closing the time index.")
        Utils.closeQuietly(txnIndex, "Closing the transaction index.")
      }
    }
  }
}

/**
 * This is a LFU (Least Frequently Used) cache of remote index files stored in `$logdir/remote-log-index-cache`.
 * This is helpful to avoid re-fetching the index files like offset, time indexes from the remote storage for every
 * fetch call. The cache is re-initialized from the index files on disk on startup, if the index files are available.
 *
 * The cache contains a garbage collection thread which will delete the files for entries that have been removed from
 * the cache.
 *
 * Note that closing this cache does not delete the index files on disk.
 * Note that the cache eviction policy is based on the default implementation of Caffeine i.e.
 * <a href="https://github.com/ben-manes/caffeine/wiki/Efficiency">Window TinyLfu</a>. TinyLfu relies on a frequency
 * sketch to probabilistically estimate the historic usage of an entry.
 *
 * @param maxSize              maximum number of segment index entries to be cached.
 * @param remoteStorageManager RemoteStorageManager instance, to be used in fetching indexes.
 * @param logDir               log directory
 */
@threadsafe
class RemoteIndexCache(maxSize: Int = 1024, remoteStorageManager: RemoteStorageManager, logDir: String)
  extends Logging with AutoCloseable {
  /**
   * Directory where the index files will be stored on disk.
   */
  private val cacheDir = new File(logDir, DirName)
  /**
   * Represents if the cache is closed or not. Closing the cache is an irreversible operation.
   */
  private val isRemoteIndexCacheClosed: AtomicBoolean = new AtomicBoolean(false)
  /**
   * Unbounded queue containing the removed entries from the cache which are waiting to be garbage collected.
   *
   * Visible for testing
   */
  private[remote] val expiredIndexes = new LinkedBlockingQueue[Entry]()
  /**
   * Lock used to synchronize close with other read operations. This ensures that when we close, we don't have any other
   * concurrent reads in-progress.
   */
  private val lock: ReentrantReadWriteLock = new ReentrantReadWriteLock()
  /**
   * Actual cache implementation that this file wraps around.
   *
   * The requirements for this internal cache is as follows:
   * 1. Multiple threads should be able to read concurrently.
   * 2. Fetch for missing keys should not block read for available keys.
   * 3. Only one thread should fetch for a specific key.
   * 4. Should support LRU-like policy.
   *
   * We use [[Caffeine]] cache instead of implementing a thread safe LRU cache on our own.
   *
   * Visible for testing.
   */
  private[remote] var internalCache: Cache[Uuid, Entry] = Caffeine.newBuilder()
    .maximumSize(maxSize)
    // removeListener is invoked when either the entry is invalidated (means manual removal by the caller) or
    // evicted (means removal due to the policy)
    .removalListener((_: Uuid, entry: Entry, _: RemovalCause) => {
      // Mark the entries for cleanup and add them to the queue to be garbage collected later by the background thread.
      entry.markForCleanup()
      if (!expiredIndexes.offer(entry)) {
        error(s"Error while inserting entry $entry into the cleaner queue")
      }
    })
    .build[Uuid, Entry]()

  private def init(): Unit = {
    try {
      Files.createDirectory(cacheDir.toPath)
      info(s"Created new file $cacheDir for RemoteIndexCache")
    } catch {
      case _: FileAlreadyExistsException =>
        info(s"RemoteIndexCache directory $cacheDir already exists. Re-using the same directory.")
      case e: Exception =>
        error(s"Unable to create directory $cacheDir for RemoteIndexCache.", e)
        throw e
    }

    // Delete any .deleted files remained from the earlier run of the broker.
    Files.list(cacheDir.toPath).forEach((path: Path) => {
      if (path.endsWith(LogFileUtils.DELETED_FILE_SUFFIX)) {
        if (Files.deleteIfExists(path))
          debug(s"Deleted file $path on cache initialization")
      }
    })

    Files.list(cacheDir.toPath).forEach((path:Path) => {
      val pathStr = path.getFileName.toString
      val name = pathStr.substring(0, pathStr.lastIndexOf("_") + 1)

      // Create entries for each path if all the index files exist.
      val firstIndex = name.indexOf('_')
      val offset = name.substring(0, firstIndex).toInt
      val uuid = Uuid.fromString(name.substring(firstIndex + 1, name.lastIndexOf('_')))

      // It is safe to update the internalCache non-atomically here since this function is always called by a single
      // thread only.
      if (!internalCache.asMap().containsKey(uuid)) {
        val offsetIndexFile = new File(cacheDir, name + UnifiedLog.IndexFileSuffix)
        val timestampIndexFile = new File(cacheDir, name + UnifiedLog.TimeIndexFileSuffix)
        val txnIndexFile = new File(cacheDir, name + UnifiedLog.TxnIndexFileSuffix)

        if (Files.exists(offsetIndexFile.toPath) &&
            Files.exists(timestampIndexFile.toPath) &&
            Files.exists(txnIndexFile.toPath)) {

          val offsetIndex = new OffsetIndex(offsetIndexFile, offset, Int.MaxValue, false)
          offsetIndex.sanityCheck()

          val timeIndex = new TimeIndex(timestampIndexFile, offset, Int.MaxValue, false)
          timeIndex.sanityCheck()

          val txnIndex = new TransactionIndex(offset, txnIndexFile)
          txnIndex.sanityCheck()

          internalCache.put(uuid, new Entry(offsetIndex, timeIndex, txnIndex))
        } else {
          // Delete all of them if any one of those indexes is not available for a specific segment id
          Files.deleteIfExists(offsetIndexFile.toPath)
          Files.deleteIfExists(timestampIndexFile.toPath)
          Files.deleteIfExists(txnIndexFile.toPath)
        }
      }
    })
  }

  init()

  // Start cleaner thread that will clean the expired entries
  private[remote] var cleanerThread: ShutdownableThread = new ShutdownableThread(remoteLogIndexCacheCleanerThread) {
    setDaemon(true)

    override def doWork(): Unit = {
      try {
        while (!isRemoteIndexCacheClosed.get()) {
          val entry = expiredIndexes.take()
          debug(s"Cleaning up index entry $entry")
          entry.cleanup()
        }
      } catch {
        case ex: InterruptedException =>
          // cleaner thread should only be interrupted when cache is being closed, else it's an error
          if (!isRemoteIndexCacheClosed.get()) {
            error("Cleaner thread received interruption but remote index cache is not closed", ex)
            throw ex
          } else {
            debug("Cleaner thread was interrupted on cache shutdown")
          }
        case ex: Exception => error("Error occurred while fetching/cleaning up expired entry", ex)
      }
    }
  }

  cleanerThread.start()

  def getIndexEntry(remoteLogSegmentMetadata: RemoteLogSegmentMetadata): Entry = {
    if (isRemoteIndexCacheClosed.get()) {
      throw new IllegalStateException(s"Unable to fetch index for " +
        s"segment id=${remoteLogSegmentMetadata.remoteLogSegmentId().id()}. Index instance is already closed.")
    }

    inReadLock(lock) {
      val cacheKey = remoteLogSegmentMetadata.remoteLogSegmentId().id()
      internalCache.get(cacheKey, (uuid: Uuid) => {
        def loadIndexFile[T](fileName: String,
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

          if (Files.exists(indexFile.toPath)) {
            try {
              readIndex(indexFile)
            } catch {
              case ex: CorruptRecordException =>
                info(s"Error occurred while loading the stored index at ${indexFile.toPath}", ex)
                fetchAndCreateIndex()
            }
          } else {
            fetchAndCreateIndex()
          }
        }

        val startOffset = remoteLogSegmentMetadata.startOffset()
        // uuid.toString uses URL encoding which is safe for filenames and URLs.
        val fileName = startOffset.toString + "_" + uuid.toString + "_"

        val offsetIndex: OffsetIndex = loadIndexFile(fileName, UnifiedLog.IndexFileSuffix,
          rlsMetadata => remoteStorageManager.fetchIndex(rlsMetadata, IndexType.OFFSET),
          file => {
            val index = new OffsetIndex(file, startOffset, Int.MaxValue, false)
            index.sanityCheck()
            index
          })

        val timeIndex: TimeIndex = loadIndexFile(fileName, UnifiedLog.TimeIndexFileSuffix,
          rlsMetadata => remoteStorageManager.fetchIndex(rlsMetadata, IndexType.TIMESTAMP),
          file => {
            val index = new TimeIndex(file, startOffset, Int.MaxValue, false)
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
    inReadLock(lock) {
      getIndexEntry(remoteLogSegmentMetadata).lookupOffset(offset).position
    }
  }

  def lookupTimestamp(remoteLogSegmentMetadata: RemoteLogSegmentMetadata, timestamp: Long, startingOffset: Long): Int = {
    inReadLock(lock) {
      getIndexEntry(remoteLogSegmentMetadata).lookupTimestamp(timestamp, startingOffset).position
    }
  }

  /**
   * Close should synchronously cleanup the resources used by this cache.
   * This index is closed when [[RemoteLogManager]] is closed.
   */
  def close(): Unit = {
    // make close idempotent and ensure no more reads allowed from henceforth. The in-progress reads will continue to
    // completion (release the read lock) and then close will begin executing. Cleaner thread will immediately stop work.
    if (!isRemoteIndexCacheClosed.getAndSet(true)) {
      inWriteLock(lock) {
        info(s"Close initiated for RemoteIndexCache. Cache stats=${internalCache.stats}. " +
          s"Cache entries pending delete=${expiredIndexes.size()}")
        // Initiate shutdown for cleaning thread
        val shutdownRequired = cleanerThread.initiateShutdown()
        // Close all the opened indexes to force unload mmap memory. This does not delete the index files from disk.
        internalCache.asMap().forEach((_, entry) => entry.close())
        // wait for cleaner thread to shutdown
        if (shutdownRequired) cleanerThread.awaitShutdown()
        // Note that internal cache does not require explicit cleaning / closing. We don't want to invalidate or cleanup
        // the cache as both would lead to triggering of removal listener.
        internalCache = null
        info(s"Close completed for RemoteIndexCache")
      }
    }
  }
}
