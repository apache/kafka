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

package org.apache.kafka.controller;

import java.util.Objects;
import org.apache.kafka.common.Uuid;

final class TopicIdPartition {
    private final Uuid topicId;
    private final int partitionId;

    TopicIdPartition(Uuid topicId, int partitionId) {
        this.topicId = topicId;
        this.partitionId = partitionId;
    }

    public Uuid topicId() {
        return topicId;
    }

    public int partitionId() {
        return partitionId;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof TopicIdPartition)) return false;
        TopicIdPartition other = (TopicIdPartition) o;
        return other.topicId.equals(topicId) && other.partitionId == partitionId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(topicId, partitionId);
    }

    @Override
    public String toString() {
        return topicId + ":" + partitionId;
    }
}
