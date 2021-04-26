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

package kafka.server

import org.apache.kafka.common.TopicPartition
import scala.collection.{Map, Set}

trait FetcherManagerTrait {
  def addFetcherForPartitions(partitionAndOffsets: Map[TopicPartition, InitialFetchState]): Unit
  def removeFetcherForPartitions(partitions: Set[TopicPartition]): Map[TopicPartition, PartitionFetchState]
  def addFailedPartition(topicPartition: TopicPartition): Unit
  private[server] def getFetcher(topicPartition: TopicPartition): Option[FetcherTrait]
  def shutdownIdleFetcherThreads(): Unit
  def shutdown(): Unit
}
