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
package org.apache.kafka.tools.consumer.group;

import org.apache.kafka.common.Node;

import java.util.Optional;

class PartitionAssignmentState {
    final String group;
    final Optional<Node> coordinator;
    final Optional<String> topic;
    final Optional<Integer> partition;
    final Optional<Long> offset;
    final Optional<Long> lag;
    final Optional<String> consumerId;
    final Optional<String> host;
    final Optional<String> clientId;
    final Optional<Long> logEndOffset;
    final Optional<Integer> leaderEpoch;

    PartitionAssignmentState(
        String group,
        Optional<Node> coordinator,
        Optional<String> topic,
        Optional<Integer> partition,
        Optional<Long> offset,
        Optional<Long> lag,
        Optional<String> consumerId,
        Optional<String> host,
        Optional<String> clientId,
        Optional<Long> logEndOffset,
        Optional<Integer> leaderEpoch
    ) {
        this.group = group;
        this.coordinator = coordinator;
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.lag = lag;
        this.consumerId = consumerId;
        this.host = host;
        this.clientId = clientId;
        this.logEndOffset = logEndOffset;
        this.leaderEpoch = leaderEpoch;
    }
}
