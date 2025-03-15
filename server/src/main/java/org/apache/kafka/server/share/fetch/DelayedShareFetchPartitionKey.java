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
package org.apache.kafka.server.share.fetch;

import org.apache.kafka.common.Uuid;

import java.util.Objects;

/**
 * A key for delayed share fetch purgatory that refers to the topic partition.
 */
public class DelayedShareFetchPartitionKey implements DelayedShareFetchKey {
    private final Uuid topicId;
    private final int partition;

    public DelayedShareFetchPartitionKey(Uuid topicId, int partition) {
        this.topicId = topicId;
        this.partition = partition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DelayedShareFetchPartitionKey that = (DelayedShareFetchPartitionKey) o;
        return topicId.equals(that.topicId) && partition == that.partition;
    }

    @Override
    public int hashCode() {
        return Objects.hash(topicId, partition);
    }

    @Override
    public String toString() {
        return "DelayedShareFetchPartitionKey(topicId=" + topicId +
            ", partition=" + partition + ")";
    }

    @Override
    public String keyLabel() {
        return String.format("topicId=%s, partition=%s", topicId, partition);
    }
}
