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

import org.apache.kafka.common.requests.JoinGroupRequest;

import java.util.Objects;
import java.util.Optional;

/**
 * A metadata struct containing the consumer group information.
 * Note: Any change to this class is considered public and requires a KIP.
 */
public class ConsumerGroupMetadata {
    final private String groupId;
    final private int generationId;
    final private String memberId;
    final private Optional<String> groupInstanceId;

    public ConsumerGroupMetadata(String groupId,
                                 int generationId,
                                 String memberId,
                                 Optional<String> groupInstanceId) {
        this.groupId = Objects.requireNonNull(groupId, "group.id can't be null");
        this.generationId = generationId;
        this.memberId = Objects.requireNonNull(memberId, "member.id can't be null");
        this.groupInstanceId = Objects.requireNonNull(groupInstanceId, "group.instance.id can't be null");
    }

    public ConsumerGroupMetadata(String groupId) {
        this(groupId,
            JoinGroupRequest.UNKNOWN_GENERATION_ID,
            JoinGroupRequest.UNKNOWN_MEMBER_ID,
            Optional.empty());
    }

    public String groupId() {
        return groupId;
    }

    public int generationId() {
        return generationId;
    }

    public String memberId() {
        return memberId;
    }

    public Optional<String> groupInstanceId() {
        return groupInstanceId;
    }

    @Override
    public String toString() {
        return String.format("GroupMetadata(groupId = %s, generationId = %d, memberId = %s, groupInstanceId = %s)",
            groupId,
            generationId,
            memberId,
            groupInstanceId.orElse(""));
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final ConsumerGroupMetadata that = (ConsumerGroupMetadata) o;
        return generationId == that.generationId &&
            Objects.equals(groupId, that.groupId) &&
            Objects.equals(memberId, that.memberId) &&
            Objects.equals(groupInstanceId, that.groupInstanceId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupId, generationId, memberId, groupInstanceId);
    }

}
