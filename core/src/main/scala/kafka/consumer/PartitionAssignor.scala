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

package kafka.consumer

import kafka.common.TopicAndPartition
import kafka.utils.{Pool, CoreUtils, ZkUtils, Logging}

import scala.collection.mutable

@deprecated("This trait has been deprecated and will be removed in a future release. " +
            "Please use org.apache.kafka.clients.consumer.internals.PartitionAssignor instead.", "0.11.0.0")
trait PartitionAssignor {

  /**
   * Assigns partitions to consumer instances in a group.
   * @return An assignment map of partition to this consumer group. This includes assignments for threads that belong
   *         to the same consumer group.
   */
  def assign(ctx: AssignmentContext): Pool[String, mutable.Map[TopicAndPartition, ConsumerThreadId]]

}

@deprecated("This object has been deprecated and will be removed in a future release. " +
            "Please use org.apache.kafka.clients.consumer.internals.PartitionAssignor instead.", "0.11.0.0")
object PartitionAssignor {
  def createInstance(assignmentStrategy: String) = assignmentStrategy match {
    case "roundrobin" => new RoundRobinAssignor()
    case "fair" => new FairAssignor()
    case _ => new RangeAssignor()
  }
}

@deprecated("This class has been deprecated and will be removed in a future release.", "0.11.0.0")
class AssignmentContext(group: String, val consumerId: String, excludeInternalTopics: Boolean, zkUtils: ZkUtils) {
  val myTopicThreadIds: collection.Map[String, collection.Set[ConsumerThreadId]] = {
    val myTopicCount = TopicCount.constructTopicCount(group, consumerId, zkUtils, excludeInternalTopics)
    myTopicCount.getConsumerThreadIdsPerTopic
  }

  val consumersForTopic: collection.Map[String, List[ConsumerThreadId]] =
    zkUtils.getConsumersPerTopic(group, excludeInternalTopics)

  // Some assignment strategies require knowledge of all topics consumed by any member of the group
  val partitionsForTopic: collection.Map[String, Seq[Int]] =
    zkUtils.getPartitionsForTopics(consumersForTopic.keySet.toSeq)

  val consumers: Seq[String] = zkUtils.getConsumersInGroup(group).sorted
}

/**
 * The round-robin partition assignor lays out all the available partitions and all the available consumer threads. It
 * then proceeds to do a round-robin assignment from partition to consumer thread. If the subscriptions of all consumer
 * instances are identical, then the partitions will be uniformly distributed. (i.e., the partition ownership counts
 * will be within a delta of exactly one across all consumer threads.)
 */
@deprecated("This class has been deprecated and will be removed in a future release. " +
            "Please use org.apache.kafka.clients.consumer.RoundRobinAssignor instead.", "0.11.0.0")
class RoundRobinAssignor() extends PartitionAssignor with Logging {

  def assign(ctx: AssignmentContext) = {

    val valueFactory = (_: String) => new mutable.HashMap[TopicAndPartition, ConsumerThreadId]
    val partitionAssignment =
      new Pool[String, mutable.Map[TopicAndPartition, ConsumerThreadId]](Some(valueFactory))

    if (ctx.consumersForTopic.nonEmpty) {
      // Collect consumer thread ids across all topics, remove duplicates, and sort to ensure determinism
      val allThreadIds = ctx.consumersForTopic.flatMap { case (topic, threadIds) =>
         threadIds
      }.toSet.toSeq.sorted

      val threadAssignor = CoreUtils.circularIterator(allThreadIds)

      info("Starting round-robin assignment with consumers " + ctx.consumers)
      val allTopicPartitions = ctx.partitionsForTopic.flatMap { case (topic, partitions) =>
        info("Consumer %s rebalancing the following partitions for topic %s: %s"
          .format(ctx.consumerId, topic, partitions))
        partitions.map(partition => {
          TopicAndPartition(topic, partition)
        })
      }.toSeq.sortWith((topicPartition1, topicPartition2) => {
        /*
         * Randomize the order by taking the hashcode to reduce the likelihood of all partitions of a given topic ending
         * up on one consumer (if it has a high enough stream count).
         */
        topicPartition1.toString.hashCode < topicPartition2.toString.hashCode
      })

      allTopicPartitions.foreach(topicPartition => {
        val threadId = threadAssignor.dropWhile(threadId => !ctx.consumersForTopic(topicPartition.topic).contains(threadId)).next
        // record the partition ownership decision
        val assignmentForConsumer = partitionAssignment.getAndMaybePut(threadId.consumer)
        assignmentForConsumer += (topicPartition -> threadId)
      })
    }

    // assign Map.empty for the consumers which are not associated with topic partitions
    ctx.consumers.foreach(consumerId => partitionAssignment.getAndMaybePut(consumerId))
    partitionAssignment
  }
}

/**
 * Range partitioning works on a per-topic basis. For each topic, we lay out the available partitions in numeric order
 * and the consumer threads in lexicographic order. We then divide the number of partitions by the total number of
 * consumer streams (threads) to determine the number of partitions to assign to each consumer. If it does not evenly
 * divide, then the first few consumers will have one extra partition. For example, suppose there are two consumers C1
 * and C2 with two streams each, and there are five available partitions (p0, p1, p2, p3, p4). So each consumer thread
 * will get at least one partition and the first consumer thread will get one extra partition. So the assignment will be:
 * p0 -> C1-0, p1 -> C1-0, p2 -> C1-1, p3 -> C2-0, p4 -> C2-1
 */
@deprecated("This class has been deprecated and will be removed in a future release. " +
            "Please use org.apache.kafka.clients.consumer.RangeAssignor instead.", "0.11.0.0")
class RangeAssignor() extends PartitionAssignor with Logging {

  def assign(ctx: AssignmentContext) = {
    val valueFactory = (_: String) => new mutable.HashMap[TopicAndPartition, ConsumerThreadId]
    val partitionAssignment =
      new Pool[String, mutable.Map[TopicAndPartition, ConsumerThreadId]](Some(valueFactory))
    for (topic <- ctx.myTopicThreadIds.keySet) {
      val curConsumers = ctx.consumersForTopic(topic)
      val curPartitions: Seq[Int] = ctx.partitionsForTopic(topic)

      val nPartsPerConsumer = curPartitions.size / curConsumers.size
      val nConsumersWithExtraPart = curPartitions.size % curConsumers.size

      info("Consumer " + ctx.consumerId + " rebalancing the following partitions: " + curPartitions +
        " for topic " + topic + " with consumers: " + curConsumers)

      for (consumerThreadId <- curConsumers) {
        val myConsumerPosition = curConsumers.indexOf(consumerThreadId)
        assert(myConsumerPosition >= 0)
        val startPart = nPartsPerConsumer * myConsumerPosition + myConsumerPosition.min(nConsumersWithExtraPart)
        val nParts = nPartsPerConsumer + (if (myConsumerPosition + 1 > nConsumersWithExtraPart) 0 else 1)

        /**
         *   Range-partition the sorted partitions to consumers for better locality.
         *  The first few consumers pick up an extra partition, if any.
         */
        if (nParts <= 0)
          warn("No broker partitions consumed by consumer thread " + consumerThreadId + " for topic " + topic)
        else {
          for (i <- startPart until startPart + nParts) {
            val partition = curPartitions(i)
            info(consumerThreadId + " attempting to claim partition " + partition)
            // record the partition ownership decision
            val assignmentForConsumer = partitionAssignment.getAndMaybePut(consumerThreadId.consumer)
            assignmentForConsumer += (TopicAndPartition(topic, partition) -> consumerThreadId)
          }
        }
      }
    }

    // assign Map.empty for the consumers which are not associated with topic partitions
    ctx.consumers.foreach(consumerId => partitionAssignment.getAndMaybePut(consumerId))
    partitionAssignment
  }
}

/**
 * The fair assignor attempts to balance partitions across consumers such that each consumer thread is assigned approximately
 * the same number of partitions, even if the consumer topic subscriptions are substantially different (if they are identical,
 * then the result will be equivalent to that of the roundrobin assignor). The running total of assignments per consumer
 * thread is tracked as the algorithm executes in order to accomplish this.
 *
 * The algorithm starts with the topic with the fewest consumer subscriptions, and assigns its partitions in roundrobin
 * fashion. In the event of a tie for least subscriptions, the topic with the highest partition count is assigned first, as
 * this generally creates a more balanced distribution. The final tiebreaker is the topic name.
 *
 * The partitions for subsequent topics are assigned to the subscribing consumer with the fewest number of assignments.
 * In the event of a tie for least assignments, the tiebreaker is the consumer id, so that the assignment pattern is fairly
 * similar to how the roundrobin assignor functions.
 *
 * For example, suppose there are two consumers C0 and C1, two topics t0 and t1, and each topic has 3 partitions,
 * resulting in partitions t0p0, t0p1, t0p2, t1p0, t1p1, and t1p2. If both C0 and C1 are consuming t0, but only C1 is
 * consuming t1 then the assignment will be:
 * C0 -> [t0p0, t0p1, t0p2]
 * C1 -> [t1p0, t1p1, t1p2]
 */
class FairAssignor() extends PartitionAssignor with Logging {

  def assign(ctx: AssignmentContext) = {
    val valueFactory = (topic: String) => new mutable.HashMap[TopicAndPartition, ConsumerThreadId]
    val partitionAssignment =
      new Pool[String, mutable.Map[TopicAndPartition, ConsumerThreadId]](Some(valueFactory))

    if (ctx.consumersForTopic.size > 0) {
      val allThreadIds = ctx.consumersForTopic.flatMap { case (topic, threadIds) => threadIds }

      // Map for tracking the total number of partitions assigned to each consumer thread
      val consumerAssignmentCounts: mutable.Map[ConsumerThreadId, Int] = mutable.Map()
      for (threadId <- allThreadIds) {
        consumerAssignmentCounts(threadId) = 0
      }

      // Assign topics with fewer consumers first, tiebreakers are most partitions, then topic name
      val topicConsumerCounts = ctx.consumersForTopic.map { case(topic, threadIds) =>
        (topic -> threadIds.size)
      }.toList.sortBy {
        count => (count._2, -ctx.partitionsForTopic(count._1).size, count._1)
      }

      val allTopicPartitions = topicConsumerCounts.flatMap { topicConsumerCount =>
        val topic = topicConsumerCount._1
        val partitions = ctx.partitionsForTopic(topic)
        info("Consumer %s rebalancing the following partitions for topic %s: %s"
            .format(ctx.consumerId, topic, partitions))
        ctx.partitionsForTopic(topic).map(partition => {
            TopicAndPartition(topic, partition)
        })
      }

      allTopicPartitions.foreach(topicPartition => {
        val topicConsumers = ctx.consumersForTopic(topicPartition.topic)
        val filteredCounts = consumerAssignmentCounts.toList.filter(consumer => topicConsumers.contains(consumer._1))

        // Assign partition to consumer thread with least assignments, tiebreaker is consumer thread id
        val threadId = filteredCounts.sortBy(count => (count._2, count._1.toString)).head._1
        consumerAssignmentCounts(threadId) += 1

        // record the partition ownership decision
        val assignmentForConsumer = partitionAssignment.getAndMaybePut(threadId.consumer)
        assignmentForConsumer += (topicPartition -> threadId)
      })
    }

    // assign Map.empty for the consumers which are not associated with topic partitions
    ctx.consumers.foreach(consumerId => partitionAssignment.getAndMaybePut(consumerId))
    partitionAssignment
  }
}
