/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import kafka.cluster.BrokerEndPoint
import kafka.server.AbstractFetcherThread.{ReplicaFetch, ResultWithPartitions}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.requests.FetchRequest
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.EpochEndOffset
import org.apache.kafka.common.message.{FetchResponseData, OffsetForLeaderEpochRequestData}
import org.apache.kafka.server.common.OffsetAndEpoch

import scala.collection.Map

/**
 * This trait defines the APIs to be used to access a broker that is a leader.
 */
trait LeaderEndPoint {

  type FetchData = FetchResponseData.PartitionData
  type EpochData = OffsetForLeaderEpochRequestData.OffsetForLeaderPartition

  /**
   * A boolean specifying if truncation when fetching from the leader is supported
   */
  def isTruncationOnFetchSupported: Boolean

  /**
   * Initiate closing access to fetches from leader.
   */
  def initiateClose(): Unit

  /**
   * Closes access to fetches from leader.
   * `initiateClose` must be called prior to invoking `close`.
   */
  def close(): Unit

  /**
   * The specific broker (host:port) we want to connect to.
   */
  def brokerEndPoint(): BrokerEndPoint

  /**
   * Given a fetchRequest, carries out the expected request and returns
   * the results from fetching from the leader.
   *
   * @param fetchRequest The fetch request we want to carry out
   *
   * @return A map of topic partition -> fetch data
   */
  def fetch(fetchRequest: FetchRequest.Builder): Map[TopicPartition, FetchData]

  /**
   * Fetches the epoch and log start offset of the given topic partition from the leader.
   *
   * @param topicPartition The topic partition that we want to fetch from
   * @param currentLeaderEpoch An int representing the current leader epoch of the requester
   *
   * @return An OffsetAndEpoch object representing the earliest offset and epoch in the leader's topic partition.
   */
  def fetchEarliestOffset(topicPartition: TopicPartition, currentLeaderEpoch: Int): OffsetAndEpoch

  /**
   * Fetches the epoch and log end offset of the given topic partition from the leader.
   *
   * @param topicPartition The topic partition that we want to fetch from
   * @param currentLeaderEpoch An int representing the current leader epoch of the requester
   *
   * @return An OffsetAndEpoch object representing the latest offset and epoch in the leader's topic partition.
   */
  def fetchLatestOffset(topicPartition: TopicPartition, currentLeaderEpoch: Int): OffsetAndEpoch

  /**
   * Fetches offset for leader epoch from the leader for each given topic partition
   *
   * @param partitions A map of topic partition -> leader epoch of the replica
   *
   * @return A map of topic partition -> end offset for a requested leader epoch
   */
  def fetchEpochEndOffsets(partitions: Map[TopicPartition, EpochData]): Map[TopicPartition, EpochEndOffset]

  /**
   * Fetches the epoch and local log start offset from the leader for the given partition and the current leader-epoch
   *
   * @param topicPartition  The topic partition that we want to fetch from
   * @param currentLeaderEpoch An int representing the current leader epoch of the requester
   *
   * @return An OffsetAndEpoch object representing the earliest local offset and epoch in the leader's topic partition.
   */
  def fetchEarliestLocalOffset(topicPartition: TopicPartition, currentLeaderEpoch: Int): OffsetAndEpoch

  /**
   * Builds a fetch request, given a partition map.
   *
   * @param partitions A map of topic partitions to their respective partition fetch state
   *
   * @return A ResultWithPartitions, used to create the fetchRequest for fetch.
   */
  def buildFetch(partitions: Map[TopicPartition, PartitionFetchState]): ResultWithPartitions[Option[ReplicaFetch]]

}
