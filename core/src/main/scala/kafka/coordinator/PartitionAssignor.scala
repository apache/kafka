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

package kafka.coordinator

import kafka.common.TopicAndPartition
import kafka.utils.CoreUtils

private[coordinator] trait PartitionAssignor {
  /**
   * Assigns partitions to consumers in a group.
   * @return A mapping from consumer to assigned partitions.
   */
  def assign(topicsPerConsumer: Map[String, Set[String]],
             partitionsPerTopic: Map[String, Int]): Map[String, Set[TopicAndPartition]]

  protected def fill[K, V](vsPerK: Map[K, Set[V]], expectedKs: Set[K]): Map[K, Set[V]] = {
    val unfilledKs = expectedKs -- vsPerK.keySet
    vsPerK ++ unfilledKs.map(k => (k, Set.empty[V]))
  }

  protected def aggregate[K, V](pairs: Seq[(K, V)]): Map[K, Set[V]] = {
    pairs
      .groupBy { case (k, v) => k }
      .map { case (k, kvPairs) => (k, kvPairs.map(_._2).toSet) }
  }

  protected def invert[K, V](vsPerK: Map[K, Set[V]]): Map[V, Set[K]] = {
    val vkPairs = vsPerK.toSeq.flatMap { case (k, vs) => vs.map(v => (v, k)) }
    aggregate(vkPairs)
  }
}

private[coordinator] object PartitionAssignor {
  val strategies = Set("range", "roundrobin")

  def createInstance(strategy: String) = strategy match {
    case "roundrobin" => new RoundRobinAssignor()
    case _ => new RangeAssignor()
  }
}

/**
 * The roundrobin assignor lays out all the available partitions and all the available consumers. It
 * then proceeds to do a roundrobin assignment from partition to consumer. If the subscriptions of all consumer
 * instances are identical, then the partitions will be uniformly distributed. (i.e., the partition ownership counts
 * will be within a delta of exactly one across all consumers.)
 *
 * For example, suppose there are two consumers C0 and C1, two topics t0 and t1, and each topic has 3 partitions,
 * resulting in partitions t0p0, t0p1, t0p2, t1p0, t1p1, and t1p2.
 *
 * The assignment will be:
 * C0 -> [t0p0, t0p2, t1p1]
 * C1 -> [t0p1, t1p0, t1p2]
 *
 * roundrobin assignment is allowed only if the set of subscribed topics is identical for every consumer within the group.
 */
private[coordinator] class RoundRobinAssignor extends PartitionAssignor {
  override def assign(topicsPerConsumer: Map[String, Set[String]],
                      partitionsPerTopic: Map[String, Int]): Map[String, Set[TopicAndPartition]] = {
    val consumersHaveIdenticalTopics = topicsPerConsumer.values.toSet.size == 1
    require(consumersHaveIdenticalTopics,
      "roundrobin assignment is allowed only if all consumers in the group subscribe to the same topics")
    val consumers = topicsPerConsumer.keys.toSeq.sorted
    val topics = topicsPerConsumer.head._2
    val consumerAssignor = CoreUtils.circularIterator(consumers)

    val allTopicPartitions = topics.toSeq.flatMap { topic =>
      val numPartitionsForTopic = partitionsPerTopic(topic)
      (0 until numPartitionsForTopic).map(partition => TopicAndPartition(topic, partition))
    }

    val consumerPartitionPairs = allTopicPartitions.map { topicAndPartition =>
      val consumer = consumerAssignor.next()
      (consumer, topicAndPartition)
    }
    fill(aggregate(consumerPartitionPairs), topicsPerConsumer.keySet)
  }
}

/**
 * The range assignor works on a per-topic basis. For each topic, we lay out the available partitions in numeric order
 * and the consumers in lexicographic order. We then divide the number of partitions by the total number of
 * consumers to determine the number of partitions to assign to each consumer. If it does not evenly
 * divide, then the first few consumers will have one extra partition.
 *
 * For example, suppose there are two consumers C0 and C1, two topics t0 and t1, and each topic has 3 partitions,
 * resulting in partitions t0p0, t0p1, t0p2, t1p0, t1p1, and t1p2.
 *
 * The assignment will be:
 * C0 -> [t0p0, t0p1, t1p0, t1p1]
 * C1 -> [t0p2, t1p2]
 */
private[coordinator] class RangeAssignor extends PartitionAssignor {
  override def assign(topicsPerConsumer: Map[String, Set[String]],
                      partitionsPerTopic: Map[String, Int]): Map[String, Set[TopicAndPartition]] = {
    val consumersPerTopic = invert(topicsPerConsumer)
    val consumerPartitionPairs = consumersPerTopic.toSeq.flatMap { case (topic, consumersForTopic) =>
      val numPartitionsForTopic = partitionsPerTopic(topic)

      val numPartitionsPerConsumer = numPartitionsForTopic / consumersForTopic.size
      val consumersWithExtraPartition = numPartitionsForTopic % consumersForTopic.size

      consumersForTopic.toSeq.sorted.zipWithIndex.flatMap { case (consumerForTopic, consumerIndex) =>
        val startPartition = numPartitionsPerConsumer * consumerIndex + consumerIndex.min(consumersWithExtraPartition)
        val numPartitions = numPartitionsPerConsumer + (if (consumerIndex + 1 > consumersWithExtraPartition) 0 else 1)

        // The first few consumers pick up an extra partition, if any.
        (startPartition until startPartition + numPartitions)
          .map(partition => (consumerForTopic, TopicAndPartition(topic, partition)))
      }
    }
    fill(aggregate(consumerPartitionPairs), topicsPerConsumer.keySet)
  }
}
