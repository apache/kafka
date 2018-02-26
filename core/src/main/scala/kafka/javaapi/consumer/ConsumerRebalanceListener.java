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
package kafka.javaapi.consumer;


import kafka.consumer.ConsumerThreadId;
import java.util.Map;
import java.util.Set;

/**
 * This listener is used for execution of tasks defined by user when a consumer rebalance
 * occurs in {@link kafka.consumer.ZookeeperConsumerConnector}
 */
/**
 * @deprecated since 0.11.0.0, this interface will be removed in a future release.
 */
@Deprecated
public interface ConsumerRebalanceListener {

    /**
     * This method is called after all the fetcher threads are stopped but before the
     * ownership of partitions are released. Depending on whether auto offset commit is
     * enabled or not, offsets may or may not have been committed.
     * This listener is initially added to prevent duplicate messages on consumer rebalance
     * in mirror maker, where offset auto commit is disabled to prevent data loss. It could
     * also be used in more general cases.
     * @param partitionOwnership The partition this consumer currently owns.
     */
    public void beforeReleasingPartitions(Map<String, Set<Integer>> partitionOwnership);

    /**
     * This method is called after the new partition assignment is finished but before fetcher
     * threads start. A map of new global partition assignment is passed in as parameter.
     * @param consumerId The consumer Id string of the consumer invoking this callback.
     * @param globalPartitionAssignment A Map[topic, Map[Partition, ConsumerThreadId]]. It is the global partition
     *                                  assignment of this consumer group.
     */
    public void beforeStartingFetchers(String consumerId, Map<String, Map<Integer, ConsumerThreadId>> globalPartitionAssignment);

}
