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

package kafka.server;

import java.util.Optional;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.FetchResponseData.PartitionData;

/**
 * This interface defines the APIs needed to handle any state transitions related to tiering
 */
public interface TierStateMachine {

    /**
     * Start the tier state machine for the provided topic partition.
     *
     * @param topicPartition the topic partition
     * @param currentFetchState the current PartitionFetchState which will
     *                          be used to derive the return value
     * @param fetchPartitionData the data from the fetch response that returned the offset moved to tiered storage error
     *
     * @return the new PartitionFetchState after the successful start of the
     *         tier state machine
     */
    PartitionFetchState start(TopicPartition topicPartition,
                              PartitionFetchState currentFetchState,
                              PartitionData fetchPartitionData) throws Exception;

    /**
     * Optionally advance the state of the tier state machine, based on the
     * current PartitionFetchState. The decision to advance the tier
     * state machine is implementation specific.
     *
     * @param topicPartition the topic partition
     * @param currentFetchState the current PartitionFetchState which will
     *                          be used to derive the return value
     *
     * @return the new PartitionFetchState if the tier state machine was advanced, otherwise, return the currentFetchState
     */
    Optional<PartitionFetchState> maybeAdvanceState(TopicPartition topicPartition,
                                                    PartitionFetchState currentFetchState);
}
