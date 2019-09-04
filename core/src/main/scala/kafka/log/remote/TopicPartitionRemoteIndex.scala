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

import java._
import java.io.{File, IOException}
import java.nio.file.{Files, Path}
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{ConcurrentNavigableMap, ConcurrentSkipListMap}
import java.util.function.{Consumer, Predicate}

import kafka.log.remote.TopicPartitionRemoteIndex.{REMOTE_OFFSET_INDEX_SUFFIX, REMOTE_TIME_INDEX_SUFFIX}
import kafka.log.{CleanableIndex, Log, OffsetIndex, TimeIndex}
import kafka.utils.{CoreUtils, Logging}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Utils

import scala.collection.JavaConverters._

class TopicPartitionRemoteIndex(val topicPartition: TopicPartition, logDir: File) extends AutoCloseable with Logging {

  private val remoteLogIndexes: ConcurrentNavigableMap[Long, RemoteLogIndex] = new ConcurrentSkipListMap[Long, RemoteLogIndex]()
  private val remoteOffsetIndexes: ConcurrentNavigableMap[Long, OffsetIndex] = new ConcurrentSkipListMap[Long, OffsetIndex]()
  private val remoteTimeIndexes: ConcurrentNavigableMap[Long, TimeIndex] = new ConcurrentSkipListMap[Long, TimeIndex]()

  @volatile private var _currentStartOffset: Option[Long] = None
  @volatile private var _currentLastOffset: Option[Long] = None

  private val lock = new ReentrantLock()

  def currentStartOffset: Option[Long] = {
    _currentStartOffset
  }

  def currentLastOffset: Option[Long] = {
    _currentLastOffset
  }

  private def addIndex(offset: Long, remoteLogIndex: RemoteLogIndex, remoteOffsetIndex: OffsetIndex,
                       remoteTimestampIndex: TimeIndex): Unit = {
    CoreUtils.inLock(lock) {
      _currentStartOffset.foreach { currentStartOffset =>
        if (currentStartOffset >= offset)
          throw new IllegalArgumentException(s"The last offset of appended log entry must increase sequentially, but " +
            s"$offset is not greater than current last offset $currentStartOffset of topic-partition $topicPartition")
      }

      remoteLogIndexes.put(offset, remoteLogIndex)
      remoteOffsetIndexes.put(offset, remoteOffsetIndex)
      remoteTimeIndexes.put(offset, remoteTimestampIndex)
      _currentStartOffset = Some(offset)
      _currentLastOffset = remoteLogIndex.lastOffset()
    }
  }

  def cleanupIndexesUntil(offset: Long): Seq[CleanableIndex] = {

    def removeIndexes[T <: CleanableIndex](indexes: ConcurrentNavigableMap[Long, T], fn: T => Any): Seq[T] = {
      val keys = indexes.headMap(offset, true).keySet().asScala
      val max = keys.max
      // do not remove it as that would remove the entry from indexes map
      // filter max key as that should not be removed
      keys.filter(key => key != max).map { key =>
        val index = indexes.remove(key)
        fn(index)
        index
      }.toSeq
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
      val removedOffsetIndexes = removeIndexes(remoteOffsetIndexes, closeAndRenameIndex)
      val removedTimeIndexes = removeIndexes(remoteTimeIndexes, closeAndRenameIndex)
      val removedRemoteLogIndexes = removeIndexes(remoteLogIndexes, closeAndRenameIndex)

      List(removedOffsetIndexes, removedTimeIndexes, removedRemoteLogIndexes).flatten
    }
  }

  def appendEntries(entries: util.List[RemoteLogIndexEntry], baseOffsetStr: String): Option[Long] = {
    val baseOffset = baseOffsetStr.toLong
    require(baseOffset >= 0, "baseOffsetStr must not be a negative number")

    val firstOffset = entries.get(0).firstOffset
    val lastOffset = entries.get(entries.size() - 1).lastOffset
    if (baseOffset > firstOffset) throw new IllegalArgumentException(
      s"base offset '$baseOffsetStr' can not be greater than start off set of the given entry $baseOffset'")

    CoreUtils.inLock(lock) {
      val resultantStartOffset: Long =
        if (currentStartOffset.isDefined) {
          if (lastOffset <= currentLastOffset.get) -1 // this means all the entries are already covered with the existing entries in index.
          else {
            //
            if (baseOffset > currentLastOffset.get) baseOffset else Math.max(firstOffset, currentLastOffset.get)
          }
        } else {
          baseOffset
        }

      logger.info(s"resultantStartOffset computed is '$resultantStartOffset' with firstOffset: '$firstOffset', " +
        s"lastOffset : '$lastOffset', currentStartOffset: $currentStartOffset , currentStartOffset: '$currentStartOffset'")

      if (resultantStartOffset >= 0) {
        val resultantStartOffsetStr = Log.filenamePrefixFromOffset(resultantStartOffset)

        val remoteLogIndex = {
          val file = new File(logDir, resultantStartOffsetStr + RemoteLogIndex.SUFFIX)
          val newlyCreated = file.createNewFile()
          if (!newlyCreated) throw new IOException("Index file: " + file + " already exists")
          new RemoteLogIndex(file, resultantStartOffset)
        }
        val remoteTimeIndex = {
          val file = new File(logDir, resultantStartOffsetStr + REMOTE_TIME_INDEX_SUFFIX)
          if (file.exists()) throw new IOException("Index file: " + file + " already exists")
          new TimeIndex(file, 0, 12 * (entries.size + 1))
        }
        val remoteOffsetIndex = {
          val file = new File(logDir, resultantStartOffsetStr + REMOTE_OFFSET_INDEX_SUFFIX)
          if (file.exists()) throw new IOException("Index file: " + file + " already exists")
          new OffsetIndex(file, resultantStartOffset, 8 * entries.size)
        }

        var position: Long = 0

        entries.iterator().asScala.filter(entry => entry.firstOffset >= resultantStartOffset)
          .foreach(entry => {
            position = remoteLogIndex.append(entry)
            remoteOffsetIndex.append(entry.firstOffset, position.toInt)
            remoteTimeIndex.maybeAppend(entry.firstTimeStamp, position)
          })

        remoteLogIndex.flush()
        remoteOffsetIndex.flush()
        remoteTimeIndex.flush()

        addIndex(resultantStartOffset, remoteLogIndex, remoteOffsetIndex, remoteTimeIndex)

        Some(resultantStartOffset)
      } else None
    }
  }

  def lookupEntryForOffset(offset: Long): Option[RemoteLogIndexEntry] = {
    val offsetEntry = remoteOffsetIndexes.floorEntry(offset)
    if (offsetEntry != null) {
      val offsetPosition = offsetEntry.getValue.lookup(offset)
      val entry = remoteLogIndexes.floorEntry(offsetPosition.offset)
      if (entry != null) entry.getValue.lookupEntry(offsetPosition.position) else None
    } else None
  }

  override def close(): Unit = {
    remoteLogIndexes.values().forEach(new Consumer[RemoteLogIndex]() {
      override def accept(x: RemoteLogIndex): Unit = Utils.closeQuietly(x, "RemoteLogIndex")
    })
    remoteOffsetIndexes.values().forEach(new Consumer[OffsetIndex] {
      override def accept(x: OffsetIndex): Unit = Utils.closeQuietly(x, "RemoteOffsetIndex")
    })
    remoteTimeIndexes.values().forEach(new Consumer[TimeIndex] {
      override def accept(x: TimeIndex): Unit = Utils.closeQuietly(x, "RemoteTimeIndex")
    })
  }
}

object TopicPartitionRemoteIndex {
  val REMOTE_TIME_INDEX_SUFFIX = ".remoteTimeIndex"
  val REMOTE_OFFSET_INDEX_SUFFIX = ".remoteOffsetIndex"

  def open(tp: TopicPartition, logDir: File): TopicPartitionRemoteIndex = {
    val entry: TopicPartitionRemoteIndex = new TopicPartitionRemoteIndex(tp, logDir)

    Files.list(logDir.toPath).filter(new Predicate[Path] {
      override def test(filePath: Path): Boolean = filePath.endsWith(RemoteLogIndex.SUFFIX)
    }).forEach(new Consumer[Path] {
      override def accept(remoteLogIndexPath: Path): Unit = {
        val file = remoteLogIndexPath.toFile
        val prefix = RemoteLogIndex.offsetFromFileName(file.getName)
        val offset = prefix.toLong
        val remoteLogIndex = RemoteLogIndex.open(file)
        val offsetIndex = new OffsetIndex(new File(logDir, prefix + REMOTE_OFFSET_INDEX_SUFFIX), offset)
        val timeIndex = new TimeIndex(new File(logDir, prefix + REMOTE_TIME_INDEX_SUFFIX), prefix.toLong)

        entry.addIndex(offset, remoteLogIndex, offsetIndex, timeIndex)
      }
    })
    entry
  }
}
