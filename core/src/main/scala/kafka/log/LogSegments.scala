/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.io.File
import java.util.Map
import java.util.concurrent.{ConcurrentNavigableMap, ConcurrentSkipListMap}

import kafka.utils.threadsafe
import org.apache.kafka.common.TopicPartition

import scala.jdk.CollectionConverters._

/**
 * This class encapsulates a thread-safe navigable map of LogSegment instances and provides the
 * required read and write behavior on the map.
 *
 * @param topicPartition the TopicPartition associated with the segments
 *                        (useful for logging purposes)
 */
class LogSegments(topicPartition: TopicPartition) {

  /* the segments of the log with key being LogSegment base offset and value being a LogSegment */
  private val segments: ConcurrentNavigableMap[Long, LogSegment] = new ConcurrentSkipListMap[Long, LogSegment]

  /**
   * @return true if the segments are empty, false otherwise.
   */
  @threadsafe
  def isEmpty: Boolean = segments.isEmpty

  /**
   * @return true if the segments are non-empty, false otherwise.
   */
  @threadsafe
  def nonEmpty: Boolean = !isEmpty

  /**
   * Add the given segment, or replace an existing entry.
   *
   * @param segment the segment to add
   */
  @threadsafe
  def add(segment: LogSegment): LogSegment = this.segments.put(segment.baseOffset, segment)

  /**
   * Remove the segment at the provided offset.
   *
   * @param offset the offset to be removed
   */
  @threadsafe
  def remove(offset: Long): Unit = segments.remove(offset)

  /**
   * Clears all entries.
   */
  @threadsafe
  def clear(): Unit = segments.clear()

  /**
   * Close all segments.
   */
  def close(): Unit = values.foreach(_.close())

  /**
   * Close the handlers for all segments.
   */
  def closeHandlers(): Unit = values.foreach(_.closeHandlers())

  /**
   * Update the directory reference for the log and indices of all segments.
   *
   * @param dir the renamed directory
   */
  def updateParentDir(dir: File): Unit = values.foreach(_.updateParentDir(dir))

  /**
   * Take care! this is an O(n) operation, where n is the number of segments.
   *
   * @return The number of segments.
   *
   */
  @threadsafe
  def numberOfSegments: Int = segments.size

  /**
   * @return the base offsets of all segments
   */
  def baseOffsets: Iterable[Long] = segments.values().asScala.map(_.baseOffset)

  /**
   * @param offset the segment to be checked
   * @return true if a segment exists at the provided offset, false otherwise.
   */
  @threadsafe
  def contains(offset: Long): Boolean = segments.containsKey(offset)

  /**
   * Retrieves a segment at the specified offset.
   *
   * @param offset the segment to be retrieved
   *
   * @return the segment if it exists, otherwise None.
   */
  @threadsafe
  def get(offset: Long): Option[LogSegment] = Option(segments.get(offset))

  /**
   * @return an iterator to the log segments ordered from oldest to newest.
   */
  def values: Iterable[LogSegment] = segments.values.asScala

  /**
   * @return An iterator to all segments beginning with the segment that includes "from" and ending
   *         with the segment that includes up to "to-1" or the end of the log (if to > end of log).
   */
  def values(from: Long, to: Long): Iterable[LogSegment] = {
    if (from == to) {
      // Handle non-segment-aligned empty sets
      List.empty[LogSegment]
    } else if (to < from) {
      throw new IllegalArgumentException(s"Invalid log segment range: requested segments in $topicPartition " +
        s"from offset $from which is greater than limit offset $to")
    } else {
      val view = Option(segments.floorKey(from)).map { floor =>
        segments.subMap(floor, to)
      }.getOrElse(segments.headMap(to))
      view.values.asScala
    }
  }

  def nonActiveLogSegmentsFrom(from: Long): Iterable[LogSegment] = {
    val activeSegment = lastSegment.get
    if (from > activeSegment.baseOffset)
      Seq.empty
    else
      values(from, activeSegment.baseOffset)
  }

  /**
   * @return the entry associated with the greatest offset less than or equal to the given offset,
   *         if it exists.
   */
  @threadsafe
  private def floorEntry(offset: Long): Option[Map.Entry[Long, LogSegment]] = Option(segments.floorEntry(offset))

  /**
   * @return the log segment with the greatest offset less than or equal to the given offset,
   *         if it exists.
   */
  @threadsafe
  def floorSegment(offset: Long): Option[LogSegment] = floorEntry(offset).map(_.getValue)

  /**
   * @return the entry associated with the greatest offset strictly less than the given offset,
   *         if it exists.
   */
  @threadsafe
  private def lowerEntry(offset: Long): Option[Map.Entry[Long, LogSegment]] = Option(segments.lowerEntry(offset))

  /**
   * @return the log segment with the greatest offset strictly less than the given offset,
   *         if it exists.
   */
  @threadsafe
  def lowerSegment(offset: Long): Option[LogSegment] = lowerEntry(offset).map(_.getValue)

  /**
   * @return the entry associated with the smallest offset strictly greater than the given offset,
   *         if it exists.
   */
  @threadsafe
  def higherEntry(offset: Long): Option[Map.Entry[Long, LogSegment]] = Option(segments.higherEntry(offset))

  /**
   * @return the log segment with the smallest offset strictly greater than the given offset,
   *         if it exists.
   */
  @threadsafe
  def higherSegment(offset: Long): Option[LogSegment]  = higherEntry(offset).map(_.getValue)

  /**
   * @return the entry associated with the smallest offset, if it exists.
   */
  @threadsafe
  def firstEntry: Option[Map.Entry[Long, LogSegment]] = Option(segments.firstEntry)

  /**
   * @return the log segment associated with the smallest offset, if it exists.
   */
  @threadsafe
  def firstSegment: Option[LogSegment] = firstEntry.map(_.getValue)

  /**
   * @return the base offset of the log segment associated with the smallest offset, if it exists
   */
  private[log] def firstSegmentBaseOffset: Option[Long] = firstSegment.map(_.baseOffset)

  /**
   * @return the entry associated with the greatest offset, if it exists.
   */
  @threadsafe
  def lastEntry: Option[Map.Entry[Long, LogSegment]] = Option(segments.lastEntry)

  /**
   * @return the log segment with the greatest offset, if it exists.
   */
  @threadsafe
  def lastSegment: Option[LogSegment] = lastEntry.map(_.getValue)

  /**
   * @return an iterable with log segments ordered from lowest base offset to highest,
   *         each segment returned has a base offset strictly greater than the provided baseOffset.
   */
  def higherSegments(baseOffset: Long): Iterable[LogSegment] = {
    val view =
      Option(segments.higherKey(baseOffset)).map {
        higherOffset => segments.tailMap(higherOffset, true)
      }.getOrElse(collection.immutable.Map[Long, LogSegment]().asJava)
    view.values.asScala
  }

  /**
   * The active segment that is currently taking appends
   */
  def activeSegment = lastSegment.get

  def sizeInBytes: Long = LogSegments.sizeInBytes(values)

  /**
   * Returns an Iterable containing segments matching the provided predicate.
   *
   * @param predicate the predicate to be used for filtering segments.
   */
  def filter(predicate: LogSegment => Boolean): Iterable[LogSegment] = values.filter(predicate)
}

object LogSegments {
  /**
   * Calculate a log's size (in bytes) from the provided log segments.
   *
   * @param segments The log segments to calculate the size of
   * @return Sum of the log segments' sizes (in bytes)
   */
  def sizeInBytes(segments: Iterable[LogSegment]): Long =
    segments.map(_.size.toLong).sum

  def getFirstBatchTimestampForSegments(segments: Iterable[LogSegment]): Iterable[Long] = {
    segments.map {
      segment =>
        segment.getFirstBatchTimestamp()
    }
  }
}
