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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.common.TopicIdPartition;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class ShareAcknowledgements {
    private final Map<TopicIdPartition, List<ShareAcknowledgementBatch>> acknowledgements;

    public static ShareAcknowledgements empty() {
        return new ShareAcknowledgements(Map.of());
    }

    public ShareAcknowledgements(Map<TopicIdPartition, List<ShareAcknowledgementBatch>> acknowledgements) {
        Objects.requireNonNull(acknowledgements, "acknowledgements cannot be null");

        Map<TopicIdPartition, List<ShareAcknowledgementBatch>> copy = new LinkedHashMap<>();
        acknowledgements.forEach((topicIdPartition, batches) -> {
            Objects.requireNonNull(topicIdPartition, "topicIdPartition cannot be null");
            Objects.requireNonNull(batches, "acknowledgement batches cannot be null");
            if (!batches.isEmpty()) {
                copy.put(topicIdPartition, List.copyOf(batches));
            }
        });
        this.acknowledgements = Map.copyOf(copy);
    }

    public boolean isEmpty() {
        return acknowledgements.isEmpty();
    }

    public Map<TopicIdPartition, List<ShareAcknowledgementBatch>> acknowledgements() {
        return acknowledgements;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ShareAcknowledgements)) return false;
        ShareAcknowledgements that = (ShareAcknowledgements) o;
        return acknowledgements.equals(that.acknowledgements);
    }

    @Override
    public int hashCode() {
        return acknowledgements.hashCode();
    }

    @Override
    public String toString() {
        return "ShareAcknowledgements(" + acknowledgements + ")";
    }
}
