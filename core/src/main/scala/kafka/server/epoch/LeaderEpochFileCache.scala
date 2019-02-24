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

import java.util.concurrent.locks.ReentrantReadWriteLock

import kafka.server.checkpoints.LeaderEpochCheckpoint
import org.apache.kafka.common.requests.EpochEndOffset._
import kafka.utils.CoreUtils._
import kafka.utils.Logging
import org.apache.kafka.common.TopicPartition

import scala.collection.mutable.ListBuffer

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
  private var epochs: ListBuffer[EpochEntry] = inWriteLock(lock) { ListBuffer(checkpoint.read(): _*) }

  /**
    * Assigns the supplied Leader Epoch to the supplied Offset
    * Once the epoch is assigned it cannot be reassigned
    */
  def assign(epoch: Int, startOffset: Long): Unit = {
    inWriteLock(lock) {
      val updateNeeded = if (epochs.isEmpty) {
        true
      } else {
        val lastEntry = epochs.last
        lastEntry.epoch != epoch || startOffset < lastEntry.startOffset
      }

      if (updateNeeded) {
        truncateAndAppend(EpochEntry(epoch, startOffset))
        flush()
      }
    }
  }

  /**
   * Remove any entries which violate monotonicity following the insertion of an assigned epoch.
   */
  private def truncateAndAppend(entryToAppend: EpochEntry): Unit = {
    validateAndMaybeWarn(entryToAppend)

    val (retainedEpochs, removedEpochs) = epochs.partition { entry =>
      entry.epoch < entryToAppend.epoch && entry.startOffset < entryToAppend.startOffset
    }

    epochs = retainedEpochs :+ entryToAppend

    if (removedEpochs.isEmpty) {
      debug(s"Appended new epoch entry $entryToAppend. Cache now contains ${epochs.size} entries.")
    } else {
      warn(s"New epoch entry $entryToAppend caused truncation of conflicting entries $removedEpochs. " +
        s"Cache now contains ${epochs.size} entries.")
    }
  }

  def nonEmpty: Boolean = inReadLock(lock) {
    epochs.nonEmpty
  }

  /**
   * Returns the current Leader Epoch if one exists. This is the latest epoch
   * which has messages assigned to it.
   */
  def latestEpoch: Option[Int] = {
    inReadLock(lock) {
      epochs.lastOption.map(_.epoch)
    }
  }

  /**
   * Get the earliest cached entry if one exists.
   */
  def earliestEntry: Option[EpochEntry] = {
    inReadLock(lock) {
      epochs.headOption
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
    * if requestedEpoch is < the first epoch cached, UNSUPPORTED_EPOCH_OFFSET will be returned
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
          val (subsequentEpochs, previousEpochs) = epochs.partition { e => e.epoch > requestedEpoch}
          if (subsequentEpochs.isEmpty) {
            // The requested epoch is larger than any known epoch. This case should never be hit because
            // the latest cached epoch is always the largest.
            (UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET)
          } else if (previousEpochs.isEmpty) {
            // The requested epoch is smaller than any known epoch, so we return the start offset of the first
            // known epoch which is larger than it. This may be inaccurate as there could have been
            // epochs in between, but the point is that the data has already been removed from the log
            // and we want to ensure that the follower can replicate correctly beginning from the leader's
            // start offset.
            (requestedEpoch, subsequentEpochs.head.startOffset)
          } else {
            // We have at least one previous epoch and one subsequent epoch. The result is the first
            // prior epoch and the starting offset of the first subsequent epoch.
            (previousEpochs.last.epoch, subsequentEpochs.head.startOffset)
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
        val (subsequentEntries, previousEntries) = epochs.partition(_.startOffset >= endOffset)
        epochs = previousEntries

        flush()

        debug(s"Cleared entries $subsequentEntries from epoch cache after " +
          s"truncating to end offset $endOffset, leaving ${epochs.size} entries in the cache.")
      }
    }
  }

  /**
    * Clears old epoch entries. This method searches for the oldest epoch < offset, updates the saved epoch offset to
    * be offset, then clears any previous epoch entries.
    *
    * This method is exclusive: so clearEarliest(6) will retain an entry at offset 6.
    *
    * @param startOffset the offset to clear up to
    */
  def truncateFromStart(startOffset: Long): Unit = {
    inWriteLock(lock) {
      if (epochs.nonEmpty) {
        val (subsequentEntries, previousEntries) = epochs.partition(_.startOffset > startOffset)

        previousEntries.lastOption.foreach { firstBeforeStartOffset =>
          val updatedFirstEntry = EpochEntry(firstBeforeStartOffset.epoch, startOffset)
          epochs = updatedFirstEntry +: subsequentEntries

          flush()

          debug(s"Cleared entries $previousEntries and rewrote first entry $updatedFirstEntry after " +
            s"truncating to start offset $startOffset, leaving ${epochs.size} in the cache.")
        }
      }
    }
  }

  /**
    * Delete all entries.
    */
  def clearAndFlush() = {
    inWriteLock(lock) {
      epochs.clear()
      flush()
    }
  }

  def clear() = {
    inWriteLock(lock) {
      epochs.clear()
    }
  }

  // Visible for testing
  def epochEntries: ListBuffer[EpochEntry] = {
    epochs
  }

  private def latestEntry: Option[EpochEntry] = epochs.lastOption

  private def flush(): Unit = {
    checkpoint.write(epochs)
  }

  private def validateAndMaybeWarn(entry: EpochEntry) = {
    if (entry.epoch < 0) {
      throw new IllegalArgumentException(s"Received invalid partition leader epoch entry $entry")
    } else {
      // If the latest append violates the monotonicity of epochs or starting offsets, our choices
      // are either to raise an error, ignore the append, or allow the append and truncate the
      // conflicting entries from the cache. Raising an error risks killing the fetcher threads in
      // pathological cases (i.e. cases we are not yet aware of). We instead take the final approach
      // and assume that the latest append is always accurate.

      latestEntry.foreach { latest =>
        if (entry.epoch < latest.epoch)
          warn(s"Received leader epoch assignment $entry which has an epoch less than the epoch " +
            s"of the latest entry $latest. This implies messages have arrived out of order.")
        else if (entry.startOffset < latest.startOffset)
          warn(s"Received leader epoch assignment $entry which has a starting offset which is less than " +
            s"the starting offset of the latest entry $latest. This implies messages have arrived out of order.")
      }
    }
  }
}

// Mapping of epoch to the first offset of the subsequent epoch
case class EpochEntry(epoch: Int, startOffset: Long) {
  override def toString: String = {
    s"EpochEntry(epoch=$epoch, startOffset=$startOffset)"
  }
}
