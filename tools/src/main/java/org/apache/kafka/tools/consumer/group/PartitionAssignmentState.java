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
import java.util.OptionalInt;
import java.util.OptionalLong;

class PartitionAssignmentState {
    public final String group;
    public final Optional<Node> coordinator;
    public final Optional<String> topic;
    public final OptionalInt partition;
    public final OptionalLong offset;
    public final OptionalLong lag;
    public final Optional<String> consumerId;
    public final Optional<String> host;
    public final Optional<String> clientId;
    public final OptionalLong logEndOffset;

    public PartitionAssignmentState(String group, Optional<Node> coordinator, Optional<String> topic,
                                    OptionalInt partition, OptionalLong offset, OptionalLong lag,
                                    Optional<String> consumerId, Optional<String> host, Optional<String> clientId,
                                    OptionalLong logEndOffset) {
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
    }
}
