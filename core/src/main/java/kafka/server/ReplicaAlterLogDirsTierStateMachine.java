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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.FetchResponseData.PartitionData;

import java.util.Optional;

/**
 The replica alter log dirs tier state machine is unsupported but is provided to the ReplicaAlterLogDirsThread.
 */
public class ReplicaAlterLogDirsTierStateMachine implements TierStateMachine {

    public PartitionFetchState start(TopicPartition topicPartition,
                                     PartitionFetchState currentFetchState,
                                     PartitionData fetchPartitionData) throws Exception {
        // JBOD is not supported with tiered storage.
        throw new UnsupportedOperationException("Building remote log aux state is not supported in ReplicaAlterLogDirsThread.");
    }

    public Optional<PartitionFetchState> maybeAdvanceState(TopicPartition topicPartition,
                                                           PartitionFetchState currentFetchState) {
        return Optional.empty();
    }
}
