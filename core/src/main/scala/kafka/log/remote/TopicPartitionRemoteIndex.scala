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
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{ConcurrentNavigableMap, ConcurrentSkipListMap}

import kafka.log.remote.TopicPartitionRemoteIndex.{REMOTE_OFFSET_INDEX_SUFFIX, REMOTE_TIME_INDEX_SUFFIX}
import kafka.log.{Log, OffsetIndex, TimeIndex}
import kafka.utils.{CoreUtils, Logging}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Utils

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

  private def addIndex(offset: Long, remoteLogIndex: RemoteLogIndex, remoteOffsetIndex: OffsetIndex, remoteTimestampIndex: TimeIndex): Unit = {
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

  def appendEntries(entries: Seq[RemoteLogIndexEntry], baseOffsetStr: String): Option[Long] = {
    val baseOffset = baseOffsetStr.toLong
    require(baseOffset >= 0, "baseOffsetStr must not be a negative number")

    val firstOffset = entries.head.firstOffset
    val lastOffset = entries.last.lastOffset
    if (baseOffset > firstOffset) throw new IllegalArgumentException(s"base offset '$baseOffsetStr' can not be greater than start off set of the given entry $baseOffset'")

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

        entries.filter(entry => entry.firstOffset >= resultantStartOffset)
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
    val offsetPosition = remoteOffsetIndexes.floorEntry(offset).getValue.lookup(offset)
    remoteLogIndexes.floorEntry(offsetPosition.offset).getValue.lookupEntry(offsetPosition.position)
  }

  override def close(): Unit = {
    remoteLogIndexes.values().forEach(x => Utils.closeQuietly(x, "RemoteLogIndex"))
    remoteOffsetIndexes.values().forEach(x => Utils.closeQuietly(x, "RemoteOffsetIndex"))
    remoteTimeIndexes.values().forEach(x => Utils.closeQuietly(x, "RemoteTimeIndex"))
  }
}

object TopicPartitionRemoteIndex {
  val REMOTE_TIME_INDEX_SUFFIX = ".remoteTimeIndex"
  val REMOTE_OFFSET_INDEX_SUFFIX = ".remoteOffsetIndex"

  def open(tp: TopicPartition, logDir: File): TopicPartitionRemoteIndex = {
    val entry: TopicPartitionRemoteIndex = new TopicPartitionRemoteIndex(tp, logDir)

    Files.list(logDir.toPath).filter((filePath: Path) => filePath.endsWith(RemoteLogIndex.SUFFIX))
      .forEach((remoteLogIndexPath: Path) => {
        val prefix = RemoteLogIndex.offsetFromFileName(remoteLogIndexPath.getFileName.toString)
        val offset = prefix.toLong
        val remoteLogIndex = RemoteLogIndex.open(remoteLogIndexPath.toFile)
        val offsetIndex = new OffsetIndex(new File(logDir, prefix + REMOTE_OFFSET_INDEX_SUFFIX), offset)
        val timeIndex = new TimeIndex(new File(logDir, prefix + REMOTE_TIME_INDEX_SUFFIX), prefix.toLong)

        entry.addIndex(offset, remoteLogIndex, offsetIndex, timeIndex)
      })
    entry
  }
}
