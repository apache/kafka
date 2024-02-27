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
package org.apache.kafka.clients.consumer.internals.events;

import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class TopicMetadataEvent extends CompletableApplicationEvent<Map<String, List<PartitionInfo>>> {

    private final Optional<String> topic;
    private final long timeoutMs;

    public TopicMetadataEvent(final long timeoutMs) {
        this(Optional.empty(), timeoutMs);
    }

    public TopicMetadataEvent(final String topic, final long timeoutMs) {
        this(Optional.of(topic), timeoutMs);
    }

    public TopicMetadataEvent(final Optional<String> topic, final long timeoutMs) {
        super(Type.TOPIC_METADATA);
        this.topic = Objects.requireNonNull(topic);
        this.timeoutMs = timeoutMs;
    }

    public Optional<String> topic() {
        return topic;
    }

    public long timeoutMs() {
        return timeoutMs;
    }

    @Override
    public String toStringBase() {
        return super.toStringBase() + ", topic=" + topic;
    }
}
