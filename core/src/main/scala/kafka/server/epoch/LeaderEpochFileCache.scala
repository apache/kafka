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
package kafka.server.epoch

import java.util
import java.util.concurrent.locks.ReentrantReadWriteLock

import kafka.server.checkpoints.LeaderEpochCheckpoint
import kafka.utils.CoreUtils._
import kafka.utils.Logging
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse.{UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET}

import scala.collection.{Seq, mutable}
import scala.jdk.CollectionConverters._

/**
 * Represents a cache of (LeaderEpoch => Offset) mappings for a particular replica.
 *
 * Leader Epoch = epoch assigned to each leader by the controller.
 * Offset = offset of the first message in each epoch.
 *
 * @param topicPartition the associated topic partition
 * @param checkpoint the checkpoint file
 * @param logEndOffset function to fetch the current log end offset
 */
class LeaderEpochFileCache(topicPartition: TopicPartition,
                           logEndOffset: () => Long,
                           checkpoint: LeaderEpochCheckpoint) extends Logging {
  this.logIdent = s"[LeaderEpochCache $topicPartition] "

  private val lock = new ReentrantReadWriteLock()
  private val epochs = new util.TreeMap[Int, EpochEntry]()

  inWriteLock(lock) {
    checkpoint.read().foreach(assign)
  }

  /**
    * Assigns the supplied Leader Epoch to the supplied Offset
    * Once the epoch is assigned it cannot be reassigned
    */
  def assign(epoch: Int, startOffset: Long): Unit = {
    val entry = EpochEntry(epoch, startOffset)
    if (assign(entry)) {
      debug(s"Appended new epoch entry $entry. Cache now contains ${epochs.size} entries.")
      flush()
    }
  }

  private def assign(entry: EpochEntry): Boolean = {
    if (entry.epoch < 0 || entry.startOffset < 0) {
      throw new IllegalArgumentException(s"Received invalid partition leader epoch entry $entry")
    }

    def isUpdateNeeded: Boolean = {
      latestEntry match {
        case Some(lastEntry) =>
          entry.epoch != lastEntry.epoch || entry.startOffset < lastEntry.startOffset
        case None =>
          true
      }
    }

    // Check whether the append is needed before acquiring the write lock
    // in order to avoid contention with readers in the common case
    if (!isUpdateNeeded)
      return false

    inWriteLock(lock) {
      if (isUpdateNeeded) {
        maybeTruncateNonMonotonicEntries(entry)
        epochs.put(entry.epoch, entry)
        true
      } else {
        false
      }
    }
  }

  /**
   * Remove any entries which violate monotonicity prior to appending a new entry
   */
  private def maybeTruncateNonMonotonicEntries(newEntry: EpochEntry): Unit = {
    val removedEpochs = removeFromEnd { entry =>
      entry.epoch >= newEntry.epoch || entry.startOffset >= newEntry.startOffset
    }

    if (removedEpochs.size > 1
      || (removedEpochs.nonEmpty && removedEpochs.head.startOffset != newEntry.startOffset)) {

      // Only log a warning if there were non-trivial removals. If the start offset of the new entry
      // matches the start offset of the removed epoch, then no data has been written and the truncation
      // is expected.
      warn(s"New epoch entry $newEntry caused truncation of conflicting entries $removedEpochs. " +
        s"Cache now contains ${epochs.size} entries.")
    }
  }

  private def removeFromEnd(predicate: EpochEntry => Boolean): Seq[EpochEntry] = {
    removeWhileMatching(epochs.descendingMap.entrySet().iterator(), predicate)
  }

  private def removeFromStart(predicate: EpochEntry => Boolean): Seq[EpochEntry] = {
    removeWhileMatching(epochs.entrySet().iterator(), predicate)
  }

  private def removeWhileMatching(
    iterator: util.Iterator[util.Map.Entry[Int, EpochEntry]],
    predicate: EpochEntry => Boolean
  ): Seq[EpochEntry] = {
    val removedEpochs = mutable.ListBuffer.empty[EpochEntry]

    while (iterator.hasNext) {
      val entry = iterator.next().getValue
      if (predicate.apply(entry)) {
        removedEpochs += entry
        iterator.remove()
      } else {
        return removedEpochs
      }
    }

    removedEpochs
  }

  def nonEmpty: Boolean = inReadLock(lock) {
    !epochs.isEmpty
  }

  def latestEntry: Option[EpochEntry] = {
    inReadLock(lock) {
      Option(epochs.lastEntry).map(_.getValue)
    }
  }

  /**
   * Returns the current Leader Epoch if one exists. This is the latest epoch
   * which has messages assigned to it.
   */
  def latestEpoch: Option[Int] = {
    latestEntry.map(_.epoch)
  }

  def previousEpoch: Option[Int] = {
    inReadLock(lock) {
      latestEntry.flatMap(entry => Option(epochs.lowerEntry(entry.epoch))).map(_.getKey)
    }
  }

  /**
   * Get the earliest cached entry if one exists.
   */
  def earliestEntry: Option[EpochEntry] = {
    inReadLock(lock) {
      Option(epochs.firstEntry).map(_.getValue)
    }
  }

  /**
    * Returns the Leader Epoch and the End Offset for a requested Leader Epoch.
    *
    * The Leader Epoch returned is the largest epoch less than or equal to the requested Leader
    * Epoch. The End Offset is the end offset of this epoch, which is defined as the start offset
    * of the first Leader Epoch larger than the Leader Epoch requested, or else the Log End
    * Offset if the latest epoch was requested.
    *
    * During the upgrade phase, where there are existing messages may not have a leader epoch,
    * if requestedEpoch is < the first epoch cached, UNDEFINED_EPOCH_OFFSET will be returned
    * so that the follower falls back to High Water Mark.
    *
    * @param requestedEpoch requested leader epoch
    * @return found leader epoch and end offset
    */
  def endOffsetFor(requestedEpoch: Int): (Int, Long) = {
    inReadLock(lock) {
      val epochAndOffset =
        if (requestedEpoch == UNDEFINED_EPOCH) {
          // This may happen if a bootstrapping follower sends a request with undefined epoch or
          // a follower is on the older message format where leader epochs are not recorded
          (UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET)
        } else if (latestEpoch.contains(requestedEpoch)) {
          // For the leader, the latest epoch is always the current leader epoch that is still being written to.
          // Followers should not have any reason to query for the end offset of the current epoch, but a consumer
          // might if it is verifying its committed offset following a group rebalance. In this case, we return
          // the current log end offset which makes the truncation check work as expected.
          (requestedEpoch, logEndOffset())
        } else {
          val higherEntry = epochs.higherEntry(requestedEpoch)
          if (higherEntry == null) {
            // The requested epoch is larger than any known epoch. This case should never be hit because
            // the latest cached epoch is always the largest.
            (UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET)
          } else {
            val floorEntry = epochs.floorEntry(requestedEpoch)
            if (floorEntry == null) {
              // The requested epoch is smaller than any known epoch, so we return the start offset of the first
              // known epoch which is larger than it. This may be inaccurate as there could have been
              // epochs in between, but the point is that the data has already been removed from the log
              // and we want to ensure that the follower can replicate correctly beginning from the leader's
              // start offset.
              (requestedEpoch, higherEntry.getValue.startOffset)
            } else {
              // We have at least one previous epoch and one subsequent epoch. The result is the first
              // prior epoch and the starting offset of the first subsequent epoch.
              (floorEntry.getValue.epoch, higherEntry.getValue.startOffset)
            }
          }
        }
      debug(s"Processed end offset request for epoch $requestedEpoch and returning epoch ${epochAndOffset._1} " +
        s"with end offset ${epochAndOffset._2} from epoch cache of size ${epochs.size}")
      epochAndOffset
    }
  }

  /**
    * Removes all epoch entries from the store with start offsets greater than or equal to the passed offset.
    */
  def truncateFromEnd(endOffset: Long): Unit = {
    inWriteLock(lock) {
      if (endOffset >= 0 && latestEntry.exists(_.startOffset >= endOffset)) {
        val removedEntries = removeFromEnd(_.startOffset >= endOffset)

        flush()

        debug(s"Cleared entries $removedEntries from epoch cache after " +
          s"truncating to end offset $endOffset, leaving ${epochs.size} entries in the cache.")
      }
    }
  }

  /**
    * Clears old epoch entries. This method searches for the oldest epoch < offset, updates the saved epoch offset to
    * be offset, then clears any previous epoch entries.
    *
    * This method is exclusive: so truncateFromStart(6) will retain an entry at offset 6.
    *
    * @param startOffset the offset to clear up to
    */
  def truncateFromStart(startOffset: Long): Unit = {
    inWriteLock(lock) {
      val removedEntries = removeFromStart { entry =>
        entry.startOffset <= startOffset
      }

      removedEntries.lastOption.foreach { firstBeforeStartOffset =>
        val updatedFirstEntry = EpochEntry(firstBeforeStartOffset.epoch, startOffset)
        epochs.put(updatedFirstEntry.epoch, updatedFirstEntry)

        flush()

        debug(s"Cleared entries $removedEntries and rewrote first entry $updatedFirstEntry after " +
          s"truncating to start offset $startOffset, leaving ${epochs.size} in the cache.")
      }
    }
  }

  /**
    * Delete all entries.
    */
  def clearAndFlush(): Unit = {
    inWriteLock(lock) {
      epochs.clear()
      flush()
    }
  }

  def clear(): Unit = {
    inWriteLock(lock) {
      epochs.clear()
    }
  }

  // Visible for testing
  def epochEntries: Seq[EpochEntry] = epochs.values.asScala.toSeq

  private def flush(): Unit = {
    checkpoint.write(epochs.values.asScala)
  }

}

// Mapping of epoch to the first offset of the subsequent epoch
case class EpochEntry(epoch: Int, startOffset: Long) {
  override def toString: String = {
    s"EpochEntry(epoch=$epoch, startOffset=$startOffset)"
  }
}
