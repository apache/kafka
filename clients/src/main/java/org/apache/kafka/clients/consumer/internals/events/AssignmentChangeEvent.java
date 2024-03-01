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

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.Map;

public class AssignmentChangeEvent extends ApplicationEvent {

    private final Map<TopicPartition, OffsetAndMetadata> offsets;
    private final long currentTimeMs;

    public AssignmentChangeEvent(final Map<TopicPartition, OffsetAndMetadata> offsets, final long currentTimeMs) {
        super(Type.ASSIGNMENT_CHANGE);
        this.offsets = Collections.unmodifiableMap(offsets);
        this.currentTimeMs = currentTimeMs;
    }

    public Map<TopicPartition, OffsetAndMetadata> offsets() {
        return offsets;
    }

    public long currentTimeMs() {
        return currentTimeMs;
    }

    @Override
    protected String toStringBase() {
        return super.toStringBase() + ", offsets=" + offsets + ", currentTimeMs=" + currentTimeMs;
    }
}
