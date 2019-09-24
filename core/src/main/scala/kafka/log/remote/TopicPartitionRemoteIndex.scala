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
import java.nio.file.{Files, Path}
import java.util.Comparator
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{ConcurrentNavigableMap, ConcurrentSkipListMap}
import java.util.function.{Consumer, Predicate}

import kafka.log.remote.TopicPartitionRemoteIndex.{REMOTE_OFFSET_INDEX_SUFFIX, REMOTE_TIME_INDEX_SUFFIX}
import kafka.log.{CleanableIndex, Log, OffsetIndex, TimeIndex}
import kafka.utils.{CoreUtils, Logging}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Utils

import scala.collection.JavaConverters._

private case class RemoteSegmentIndex(offsetIndex: OffsetIndex, timeIndex: TimeIndex, remoteLogIndex: RemoteLogIndex) {
  def asList(): Seq[CleanableIndex] = {
    List(offsetIndex, timeIndex, remoteLogIndex)
  }
}

/**
 * This class contains all the indexes related to remote log segments for a given topic partition.
 *  - remote log index
 *    - maintains a mapping between offset/timestamp of logs and the location of the remote segment where the data exists.
 *  - offset index
 *    - maintains a mapping between offset and position in the remote log index which contains remote log information for this offset.
 *  - time index
 *   - maintains a mapping between timestamp and position in the remote log index which contains remote log information for this timestamp.
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

  private def addIndex(offset: Long, remoteLogIndex: RemoteLogIndex, remoteOffsetIndex: OffsetIndex,
                       remoteTimestampIndex: TimeIndex): Unit = {
    CoreUtils.inLock(lock) {
      remoteSegmentIndexes.put(offset, RemoteSegmentIndex(remoteOffsetIndex, remoteTimestampIndex, remoteLogIndex))
      if(_startOffset.isEmpty) _startOffset = Some(offset)
      _lastBatchStartOffset = Some(offset)
      _lastOffset = remoteLogIndex.lastOffset
    }
  }

  def cleanupIndexesUntil(offset: Long): Seq[CleanableIndex] = {

    def removeIndexes(indexes: ConcurrentNavigableMap[Long, RemoteSegmentIndex],
                      fn: CleanableIndex => Any): Seq[CleanableIndex] = {
      val keys = indexes.headMap(offset, true).keySet().asScala
      if(keys.isEmpty) {
        Seq.empty
      } else {
        val max = keys.max
        // do not remove it as that would remove the entry from indexes map
        // filter max key as that should not be removed
        keys.filter(key => key != max).flatMap { key =>
          val index = indexes.remove(key)
          val internalIndexes = index.asList()
          internalIndexes.foreach { x => fn(x) }
          internalIndexes
        }.toSeq
      }
    }

    def closeAndRenameIndex(index: CleanableIndex): Unit = {
      Utils.closeQuietly(index, index.getClass.getSimpleName)
      try {
        index.renameTo(new File(CoreUtils.replaceSuffix(index.file.getPath, "", Log.DeletedFileSuffix)))
      } catch {
        case ex: IOException => warn(s"remoteLogIndex with file: ${index.file} could not be renamed", ex)
      }
    }

    CoreUtils.inLock(lock) {
      // get the entries which have key <= the given offset
      removeIndexes(remoteSegmentIndexes, closeAndRenameIndex)
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
          new RemoteLogIndex(file, baseOffset)
        }
        val remoteTimeIndex = {
          val file = new File(logDir, resultantStartOffsetStr + REMOTE_TIME_INDEX_SUFFIX)
          if (file.exists()) throw new IOException("Index file: " + file + " already exists")
          new TimeIndex(file, 0, 12 * (filteredEntries.size + 1))
        }
        val remoteOffsetIndex = {
          val file = new File(logDir, resultantStartOffsetStr + REMOTE_OFFSET_INDEX_SUFFIX)
          if (file.exists()) throw new IOException("Index file: " + file + " already exists")
          new OffsetIndex(file, baseOffset, 8 * filteredEntries.size)
        }

        val positions = remoteLogIndex.append(filteredEntries)
        (filteredEntries.zip(positions)).foreach {
          case (entry, position) =>
            remoteOffsetIndex.append(entry.firstOffset, position.toInt)
            remoteTimeIndex.maybeAppend(entry.firstTimeStamp, position)
        }

        remoteLogIndex.flush()
        remoteOffsetIndex.flush()
        remoteTimeIndex.flush()

        addIndex(resultantStartOffset, remoteLogIndex, remoteOffsetIndex, remoteTimeIndex)

        Some(resultantStartOffset)
      } else None
    }
  }

  def lookupEntryForOffset(offset: Long): Option[RemoteLogIndexEntry] = {
    val segEntry = remoteSegmentIndexes.floorEntry(offset)
    if (segEntry != null) {
      val segIndex = segEntry.getValue
      val offsetPosition = segIndex.offsetIndex.lookup(offset)
      segIndex.remoteLogIndex.lookupEntry(offsetPosition.position)
    } else None
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

  def open(tp: TopicPartition, logDir: File): TopicPartitionRemoteIndex = {
    val entry: TopicPartitionRemoteIndex = new TopicPartitionRemoteIndex(tp, logDir)

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

        entry.addIndex(offset, remoteLogIndex, offsetIndex, timeIndex)
      }
    })
    entry
  }
}
