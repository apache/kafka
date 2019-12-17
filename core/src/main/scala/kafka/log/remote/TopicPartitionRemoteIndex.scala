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

import java.io.{File, FilenameFilter, IOException}
import java.nio.file.{Files, Path}
import java.util.Comparator
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{ConcurrentNavigableMap, ConcurrentSkipListMap}
import java.util.function.{Consumer, Predicate}

import kafka.log.remote.TopicPartitionRemoteIndex.{REMOTE_OFFSET_INDEX_SUFFIX, REMOTE_TIME_INDEX_SUFFIX, REMOTE_LOG_INDEX_CHECKPOINT_SUFFIX}
import kafka.log.{CleanableIndex, Log, OffsetIndex, TimeIndex}
import kafka.utils.{CoreUtils, Logging, nonthreadsafe}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Utils

import scala.collection.JavaConverters._

private class RemoteSegmentIndex(val offsetIndex: OffsetIndex, val timeIndex: TimeIndex, val remoteLogIndex: RemoteLogIndex) {
  /* The maximum timestamp we see so far */
  @volatile private var _maxTimestampSoFar: Option[Long] = None
  def maxTimestampSoFar_=(timestamp: Long): Unit = _maxTimestampSoFar = Some(timestamp)
  def maxTimestampSoFar: Long = {
    if (_maxTimestampSoFar.isEmpty)
      _maxTimestampSoFar = Some(timeIndex.lastEntry.timestamp)
    _maxTimestampSoFar.get
  }

  def asList(): Seq[CleanableIndex] = {
    List(offsetIndex, timeIndex, remoteLogIndex)
  }

  @nonthreadsafe
  def append(entries: Seq[RemoteLogIndexEntry]): Unit = {
    val positions = remoteLogIndex.append(entries)

    (entries.zip(positions)).foreach {
      case (entry, position) =>
        offsetIndex.append(entry.lastOffset, position.toInt)
        if (entry.lastTimeStamp > maxTimestampSoFar) {
          timeIndex.maybeAppend(entry.lastTimeStamp, entry.lastOffset)
          maxTimestampSoFar = entry.lastTimeStamp
        }
    }
  }

  @nonthreadsafe
  def onBecomeInactive(): Unit = {
    offsetIndex.trimToValidSize()
    timeIndex.trimToValidSize()
    remoteLogIndex.flush()
  }

  @nonthreadsafe
  def lastOffset(): Option[Long] = remoteLogIndex.lastOffset
}

/**
 * This class contains all the indexes related to remote log segments for a given topic partition.
 *  - remote log index
 *    - maintains a mapping between offset/timestamp of logs and the location of the remote segment where the data exists.
 *  - offset index
 *    - maintains a mapping between offset and position in the remote log index which contains remote log information for this offset.
 *  - time index
 *   - maintains a mapping between timestamp and message offset for the remote messages.
 *
 *   These index files are also locally stored in respective log-dir of the topic partition.
 *
 * @param topicPartition
 * @param logDir
 */
class TopicPartitionRemoteIndex(val topicPartition: TopicPartition, logDir: File) extends AutoCloseable with Logging {

  private val remoteSegmentIndexes: ConcurrentNavigableMap[Long, RemoteSegmentIndex] = new ConcurrentSkipListMap[Long, RemoteSegmentIndex]()

  @volatile private var _startOffset: Option[Long] = None
  @volatile private var _lastBatchStartOffset: Option[Long] = None
  @volatile private var _lastOffset: Option[Long] = None

  private val lock = new ReentrantLock()

  def startOffset: Option[Long] = {
    _startOffset
  }

  def lastBatchStartOffset: Option[Long] = {
    _lastBatchStartOffset
  }

  def lastOffset: Option[Long] = {
    _lastOffset
  }

  private def addIndex(offset: Long, remoteIndex: RemoteSegmentIndex): Unit = {
    CoreUtils.inLock(lock) {
      remoteSegmentIndexes.put(offset, remoteIndex)
      if(_startOffset.isEmpty) _startOffset = Some(offset)
      _lastBatchStartOffset = Some(offset)
      _lastOffset = remoteIndex.lastOffset
    }
  }

  def cleanupIndexesUntil(offset: Long): Iterable[CleanableIndex] = {

    def removeIndexes(toOffset:Long, indexes: ConcurrentNavigableMap[Long, RemoteSegmentIndex]): Iterable[CleanableIndex] = {
      if(offset < 0) {
        val result = indexes.values().asScala.toList.flatMap(x => x.asList())
        indexes.clear()
        result
      } else {
        val keys = indexes.headMap(toOffset, true).keySet().asScala
        if (keys.isEmpty) {
          Seq.empty
        } else {
          val max = keys.max
          // do not remove it as that would remove the entry from indexes map
          // filter max key as that should not be removed because that index entry will contain offsets >= they key.
          keys.filter(key => key != max).flatMap { key =>
            val index = indexes.remove(key)
            index.asList()
          }.toSeq
        }
      }
    }

    CoreUtils.inLock(lock) {
      // get the entries which have key <= the given offset
      val removedIndexes = removeIndexes(offset, remoteSegmentIndexes)
      removedIndexes.foreach { index => {
        try {
          index.renameTo(new File(CoreUtils.replaceSuffix(index.file.getPath, "", Log.DeletedFileSuffix)))
        } catch {
          case ex: IOException => warn(s"remoteLogIndex with file: ${index.file} could not be renamed", ex)
        }
      }
      }
      removedIndexes
    }
  }

  def appendEntries(entries: Seq[RemoteLogIndexEntry], baseOffsetStr: String): Option[Long] = {
    val baseOffset = baseOffsetStr.toLong
    require(baseOffset >= 0, "baseOffsetStr must not be a negative number")

    val firstEntryOffset = entries.head.firstOffset
    val lastEntryOffset = entries.last.lastOffset
    if (baseOffset > firstEntryOffset) throw new IllegalArgumentException(
      s"base offset '$baseOffsetStr' can not be greater than start off set of the given entry $baseOffset'")

    CoreUtils.inLock(lock) {
      val resultantStartOffset: Long =
        if (lastBatchStartOffset.isDefined) {
          if (lastEntryOffset <= lastOffset.get) -1 // this means all the entries are already covered with the existing entries in index.
          else {
            if (baseOffset > lastOffset.get) baseOffset else Math.max(firstEntryOffset, lastOffset.get)
          }
        } else {
          baseOffset
        }

      logger.info(s"resultantStartOffset computed is '$resultantStartOffset' with firstEntryOffset: '$firstEntryOffset', " +
        s"lastEntryOffset : '$lastEntryOffset', lastBatchStartOffset: $lastBatchStartOffset startOffset: $startOffset")

      if (resultantStartOffset >= 0) {
        val resultantStartOffsetStr = Log.filenamePrefixFromOffset(resultantStartOffset)
        val filteredEntries = entries.filter(entry => entry.firstOffset >= resultantStartOffset)

        val remoteLogIndex = {
          val file = new File(logDir, resultantStartOffsetStr + RemoteLogIndex.SUFFIX)
          val newlyCreated = file.createNewFile()
          if (!newlyCreated) throw new IOException("Index file: " + file + " already exists")
          new RemoteLogIndex(file, resultantStartOffset)
        }
        val remoteTimeIndex = {
          val file = new File(logDir, resultantStartOffsetStr + REMOTE_TIME_INDEX_SUFFIX)
          if (file.exists()) throw new IOException("Index file: " + file + " already exists")
          new TimeIndex(file, resultantStartOffset, 12 * (filteredEntries.size + 1))
        }
        val remoteOffsetIndex = {
          val file = new File(logDir, resultantStartOffsetStr + REMOTE_OFFSET_INDEX_SUFFIX)
          if (file.exists()) throw new IOException("Index file: " + file + " already exists")
          new OffsetIndex(file, resultantStartOffset, 8 * filteredEntries.size)
        }

        val newRemoteIndex = new RemoteSegmentIndex(remoteOffsetIndex, remoteTimeIndex, remoteLogIndex)

        newRemoteIndex.append(filteredEntries)
        newRemoteIndex.onBecomeInactive()
        writeCheckpoint(resultantStartOffset)

        addIndex(resultantStartOffset, newRemoteIndex)

        Some(resultantStartOffset)
      } else None
    }
  }

  def writeCheckpoint(baseOffset: Long): Unit = {
    val file = new File(logDir, Log.filenamePrefixFromOffset(baseOffset) + REMOTE_LOG_INDEX_CHECKPOINT_SUFFIX)
    if (file.createNewFile()) {
      // delete older checkpoint files
      listCheckpointFiles(baseOffset).foreach(file => {
        try {
          if (file.exists && file.isFile)
            file.delete
        } catch {
          case _: IOException =>
            logger.info("Failed to delete old remote index checkpoint file: " + file.getAbsolutePath);
        }
      })
    }
  }

  def listCheckpointFiles(beforeOffset: Long = Long.MaxValue): Array[File] = {
    logDir.listFiles(new FilenameFilter {
      override def accept(dir: File, name: String): Boolean = {
        try {
          if (name.endsWith(REMOTE_LOG_INDEX_CHECKPOINT_SUFFIX) && Log.offsetFromFileName(name) < beforeOffset)
            return true
        } catch {
          case _: NumberFormatException =>
        }
        false
      }
    })
  }

  /**
   * Delete the local remote index files that may have not be flushed before the broker shutdown.
   */
  def removeUnflushedIndexFiles(): Unit = {
    val checkpointFiles = listCheckpointFiles()
    val checkpoint = if(checkpointFiles.isEmpty) 0L else checkpointFiles.map(Log.offsetFromFile(_)).max

    logger.info(s"The maximum remote index checkpoint of $topicPartition is $checkpoint")

    // Instead of trying to recover the unflushed remote index of each topic partition, we simply delete the files
    // and rebuild it from the remote data later.
    logDir.listFiles(new FilenameFilter {
      override def accept(dir: File, name: String): Boolean = {
        try {
          if (name.endsWith(RemoteLogIndex.SUFFIX) || name.endsWith(REMOTE_OFFSET_INDEX_SUFFIX) || name.endsWith(REMOTE_TIME_INDEX_SUFFIX)) {
            if (Log.offsetFromFileName(name) >= checkpoint)
              return true
          }
        } catch {
          case _: Exception =>
        }
        false
      }
    }).foreach(f => {
      try {
        if (f.exists && f.isFile) {
          logger.info("Deleting unflushed remote index file " + f.getAbsolutePath)
          if(!f.delete)
            logger.warn("Failed to delete unflushed remote index file: " + f.getAbsolutePath);
        }
      } catch {
        case _: IOException =>
          logger.warn("Failed to delete unflushed remote index file: " + f.getAbsolutePath);
      }
    })
  }

  def lookupEntryForOffset(offset: Long): Option[RemoteLogIndexEntry] = {
    var segEntry = remoteSegmentIndexes.floorEntry(offset)
    while (segEntry != null && segEntry.getValue.lastOffset.get < offset) {
      segEntry = remoteSegmentIndexes.higherEntry(segEntry.getKey)
    }

    if (segEntry != null) {
      val segIndex = segEntry.getValue
      val offsetPosition = segIndex.offsetIndex.lookup(offset)
      var pos = offsetPosition.position

      while (true) {
        val entry = segIndex.remoteLogIndex.lookupEntry(pos)
        if (entry.isEmpty)
          return None

        if (entry.get.lastOffset >= offset)
          return entry
        else
          pos += entry.get.totalLength
      }
      None
    } else None
  }

  def lookupEntryForTimestamp(targetTimestamp: Long, startingOffset: Long): Option[RemoteLogIndexEntry] = {
    CoreUtils.inLock(lock) {
      val floorOffset = remoteSegmentIndexes.floorKey(startingOffset)
      remoteSegmentIndexes.tailMap(floorOffset).values().forEach(seg => {
        // Find the earliest remote index segment that contains the required messages
        if (seg.maxTimestampSoFar >= targetTimestamp && seg.lastOffset.get >= startingOffset) {
          // Get the earliest index entry that its timestamp is less than or equal to the target timestamp
          val timestampOffset = seg.timeIndex.lookup(targetTimestamp)
          val offset = math.max(timestampOffset.offset, startingOffset)
          val offsetPosition = seg.offsetIndex.lookup(offset)

          var pos = offsetPosition.position

          while (true) {
            val entry = seg.remoteLogIndex.lookupEntry(pos)
            if (entry.isEmpty)
              return None

            if (entry.get.lastTimeStamp >= targetTimestamp && entry.get.lastOffset >= startingOffset)
              return entry
            else
              pos += entry.get.totalLength
          }
          None
        }
      })
    }
    None
  }

  override def close(): Unit = {
    remoteSegmentIndexes.values().forEach(new Consumer[RemoteSegmentIndex]() {
      override def accept(remoteSegmentIndex: RemoteSegmentIndex): Unit =
        remoteSegmentIndex.asList().foreach { index => Utils.closeQuietly(index, "RemoteLogIndex") }
    })
  }
}

object TopicPartitionRemoteIndex {
  val REMOTE_TIME_INDEX_SUFFIX = ".remoteTimeIndex"
  val REMOTE_OFFSET_INDEX_SUFFIX = ".remoteOffsetIndex"
  /* File name suffix of remote log index checkpoint file.
   * A checkpoint file "<offset>.remoteIndexCheckpoint" indicates the remote log index files before the offset
   * have already been flushed to disk.
   */
  val REMOTE_LOG_INDEX_CHECKPOINT_SUFFIX = ".remoteIndexCheckpoint"

  def open(tp: TopicPartition, logDir: File): TopicPartitionRemoteIndex = {
    val entry: TopicPartitionRemoteIndex = new TopicPartitionRemoteIndex(tp, logDir)

    entry.removeUnflushedIndexFiles()

    Files.list(logDir.toPath).filter(new Predicate[Path] {
      // `endsWith` should be checked on `filePath.toString` instead of `filePath` as that would check
      // Path#endsWith which has respective Path semantics instead of simple string comparision.
      override def test(filePath: Path): Boolean = filePath !=null && filePath.toString.endsWith(RemoteLogIndex.SUFFIX)
    }).sorted(new Comparator[Path] {
      override def compare(path1: Path, path2: Path): Int = {
        val fileName1 = path1.getFileName
        val fileName2 = path2.getFileName
        
        if(fileName1 != null && fileName2!= null) fileName1.compareTo(fileName2)
        else throw new IllegalArgumentException("path is null")
      }
    }).forEach(new Consumer[Path] {
      override def accept(remoteLogIndexPath: Path): Unit = {
        val file = remoteLogIndexPath.toFile
        val prefix = RemoteLogIndex.fileNamePrefix(file.getName)
        val offset = prefix.toLong
        val remoteLogIndex = RemoteLogIndex.open(file)
        val offsetIndex = new OffsetIndex(new File(logDir, prefix + REMOTE_OFFSET_INDEX_SUFFIX), offset)
        val timeIndex = new TimeIndex(new File(logDir, prefix + REMOTE_TIME_INDEX_SUFFIX), offset)

        entry.addIndex(offset, new RemoteSegmentIndex(offsetIndex, timeIndex, remoteLogIndex))
      }
    })
    entry
  }
}
