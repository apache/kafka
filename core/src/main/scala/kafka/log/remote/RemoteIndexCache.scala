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
import kafka.log.remote.RemoteIndexCache.{DirName, offsetFromRemoteIndexFileName, RemoteLogIndexCacheCleanerThread, remoteLogSegmentIdFromRemoteIndexFileName, remoteOffsetIndexFile, remoteTimeIndexFile, remoteTransactionIndexFile}
import kafka.utils.CoreUtils.{inReadLock, inWriteLock}
import kafka.utils.{CoreUtils, Logging, threadsafe}
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.errors.CorruptRecordException
import org.apache.kafka.common.utils.{Utils, Time}
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
  val RemoteLogIndexCacheCleanerThread = "remote-log-index-cleaner"

  def remoteLogSegmentIdFromRemoteIndexFileName(fileName: String): Uuid = {
    val underscoreIndex = fileName.indexOf("_")
    val dotIndex = fileName.indexOf(".")
    Uuid.fromString(fileName.substring(underscoreIndex + 1, dotIndex))
  }

  def offsetFromRemoteIndexFileName(fileName: String): Long = {
    fileName.substring(0, fileName.indexOf("_")).toLong
  }

  /**
   * Generates prefix for file name for the on-disk representation of remote indexes.
   *
   * Example of file name prefix is 45_dsdsd where 45 represents the base offset for the segment and
   * sdsdsd represents the unique [[RemoteLogSegmentId]]
   *
   * @param remoteLogSegmentMetadata remote segment for the remote indexes
   * @return string which should be used as prefix for on-disk representation of remote indexes
   */
  private def generateFileNamePrefixForIndex(remoteLogSegmentMetadata: RemoteLogSegmentMetadata): String = {
    val startOffset = remoteLogSegmentMetadata.startOffset
    val segmentId = remoteLogSegmentMetadata.remoteLogSegmentId().id
    // uuid.toString uses URL encoding which is safe for filenames and URLs.
    s"${startOffset.toString}_${segmentId.toString}"
  }

  def remoteOffsetIndexFile(dir: File, remoteLogSegmentMetadata: RemoteLogSegmentMetadata): File = {
    new File(dir, remoteOffsetIndexFileName(remoteLogSegmentMetadata))
  }

  def remoteOffsetIndexFileName(remoteLogSegmentMetadata: RemoteLogSegmentMetadata): String = {
    val prefix = generateFileNamePrefixForIndex(remoteLogSegmentMetadata)
    prefix + UnifiedLog.IndexFileSuffix
  }

  def remoteTimeIndexFile(dir: File, remoteLogSegmentMetadata: RemoteLogSegmentMetadata): File = {
    new File(dir, remoteTimeIndexFileName(remoteLogSegmentMetadata))
  }

  def remoteTimeIndexFileName(remoteLogSegmentMetadata: RemoteLogSegmentMetadata): String = {
    val prefix = generateFileNamePrefixForIndex(remoteLogSegmentMetadata)
    prefix + UnifiedLog.TimeIndexFileSuffix
  }

  def remoteTransactionIndexFile(dir: File, remoteLogSegmentMetadata: RemoteLogSegmentMetadata): File = {
    new File(dir, remoteTransactionIndexFileName(remoteLogSegmentMetadata))
  }

  def remoteTransactionIndexFileName(remoteLogSegmentMetadata: RemoteLogSegmentMetadata): String = {
    val prefix = generateFileNamePrefixForIndex(remoteLogSegmentMetadata)
    prefix + UnifiedLog.TxnIndexFileSuffix
  }
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
        Utils.closeQuietly(offsetIndex, "offset index")
        Utils.closeQuietly(timeIndex, "time index")
        Utils.closeQuietly(txnIndex, "transaction index")
      }
    }
  }

  override def toString: String = {
    s"RemoteIndexCacheEntry(" +
      s"timeIndex=${timeIndex.file.getName}, " +
      s"txnIndex=${txnIndex.file.getName}, " +
      s"offsetIndex=${offsetIndex.file.getName})"
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
    .removalListener((key: Uuid, entry: Entry, _: RemovalCause) => {
      // Mark the entries for cleanup and add them to the queue to be garbage collected later by the background thread.
      entry.markForCleanup()
      if (!expiredIndexes.offer(entry)) {
        error(s"Error while inserting entry $entry for key $key into the cleaner queue because queue is full.")
      }
    })
    .build[Uuid, Entry]()

  private def init(): Unit = {
    val start = Time.SYSTEM.hiResClockMs()
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
      val indexFileName = path.getFileName.toString
      val uuid = remoteLogSegmentIdFromRemoteIndexFileName(indexFileName)
      // It is safe to update the internalCache non-atomically here since this function is always called by a single
      // thread only.
      if (!internalCache.asMap().containsKey(uuid)) {
        val fileNameWithoutDotExtensions = indexFileName.substring(0, indexFileName.indexOf("."))
        val offsetIndexFile = new File(cacheDir, fileNameWithoutDotExtensions + UnifiedLog.IndexFileSuffix)
        val timestampIndexFile = new File(cacheDir, fileNameWithoutDotExtensions + UnifiedLog.TimeIndexFileSuffix)
        val txnIndexFile = new File(cacheDir, fileNameWithoutDotExtensions + UnifiedLog.TxnIndexFileSuffix)

        // Create entries for each path if all the index files exist.
        if (Files.exists(offsetIndexFile.toPath) &&
            Files.exists(timestampIndexFile.toPath) &&
            Files.exists(txnIndexFile.toPath)) {

          val offset = offsetFromRemoteIndexFileName(indexFileName)
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
    info(s"RemoteIndexCache starts up in ${Time.SYSTEM.hiResClockMs() - start} ms.")
  }

  init()

  // Start cleaner thread that will clean the expired entries
  private[remote] var cleanerThread: ShutdownableThread = new ShutdownableThread(RemoteLogIndexCacheCleanerThread) {
    setDaemon(true)

    override def doWork(): Unit = {
      var expiredEntryOpt: Option[Entry] = None
      try {
        expiredEntryOpt = Some(expiredIndexes.take())
        expiredEntryOpt.foreach( expiredEntry => {
          log.debug(s"Cleaning up index entry $expiredEntry")
          expiredEntry.cleanup()
        })
      } catch {
        case ie: InterruptedException =>
          // cleaner thread should only be interrupted when cache is being closed, else it's an error
          if (!isRemoteIndexCacheClosed.get()) {
            log.error("Cleaner thread received interruption but remote index cache is not closed", ie)
            // propagate the InterruptedException outside to correctly close the thread.
            throw ie
          } else {
            log.debug("Cleaner thread was interrupted on cache shutdown")
          }
        // do not exit for exceptions other than InterruptedException
        case ex: Throwable => log.error(s"Error occurred while cleaning up expired entry $expiredEntryOpt", ex)
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
      // while this thread was waiting for lock, another thread may have changed the value of isRemoteIndexCacheClosed.
      // check for index close again
      if (isRemoteIndexCacheClosed.get()) {
        throw new IllegalStateException(s"Unable to fetch index for " +
          s"segment id=${remoteLogSegmentMetadata.remoteLogSegmentId().id()}. Index instance is already closed.")
      }

      val cacheKey = remoteLogSegmentMetadata.remoteLogSegmentId().id()
      internalCache.get(cacheKey, (_: Uuid) => {
        def loadIndexFile[T](indexFile: File,
                             fetchRemoteIndex: RemoteLogSegmentMetadata => InputStream,
                             readIndex: File => T): T = {
          def fetchAndCreateIndex(): T = {
            val tmpIndexFile = new File(cacheDir, indexFile.getName + RemoteIndexCache.TmpFileSuffix)

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
        val offsetIndexFile = remoteOffsetIndexFile(cacheDir, remoteLogSegmentMetadata)
        val offsetIndex: OffsetIndex = loadIndexFile(offsetIndexFile,
          rlsMetadata => remoteStorageManager.fetchIndex(rlsMetadata, IndexType.OFFSET),
          file => {
            val index = new OffsetIndex(file, startOffset, Int.MaxValue, false)
            index.sanityCheck()
            index
          })

        val timeIndexFile = remoteTimeIndexFile(cacheDir, remoteLogSegmentMetadata)
        val timeIndex: TimeIndex = loadIndexFile(timeIndexFile,
          rlsMetadata => remoteStorageManager.fetchIndex(rlsMetadata, IndexType.TIMESTAMP),
          file => {
            val index = new TimeIndex(file, startOffset, Int.MaxValue, false)
            index.sanityCheck()
            index
          })

        val txnIndexFile = remoteTransactionIndexFile(cacheDir, remoteLogSegmentMetadata)
        val txnIndex: TransactionIndex = loadIndexFile(txnIndexFile,
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
        val shutdownRequired = cleanerThread.initiateShutdown
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