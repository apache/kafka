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
package kafka.producer

import collection.mutable.Map
import collection.SortedSet
import kafka.cluster.{Broker, Partition}

trait BrokerPartitionInfo {
  /**
   * Return a sequence of (brokerId, numPartitions).
   * @param topic the topic for which this information is to be returned
   * @return a sequence of (brokerId, numPartitions). Returns a zero-length
   * sequence if no brokers are available.
   */  
  def getBrokerPartitionInfo(topic: String = null): SortedSet[Partition]

  /**
   * Generate the host and port information for the broker identified
   * by the given broker id 
   * @param brokerId the broker for which the info is to be returned
   * @return host and port of brokerId
   */
  def getBrokerInfo(brokerId: Int): Option[Broker]

  /**
   * Generate a mapping from broker id to the host and port for all brokers
   * @return mapping from id to host and port of all brokers
   */
  def getAllBrokerInfo: Map[Int, Broker]

  /**
   * This is relevant to the ZKBrokerPartitionInfo. It updates the ZK cache
   * by reading from zookeeper and recreating the data structures. This API
   * is invoked by the producer, when it detects that the ZK cache of
   * ZKBrokerPartitionInfo is stale.
   *
   */
  def updateInfo

  /**
   * Cleanup
   */
  def close
}
