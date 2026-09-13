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

import java.util.Objects;

/**
 * Metadata for the share group member obtained via {@link ShareConsumer#shareGroupMetadata()}.
 * Pass this to {@link org.apache.kafka.clients.producer.Producer#sendShareAcknowledgementsToTransaction}
 * to atomically bind acknowledgments to a producer transaction (KIP-1289).
 */
public class ShareGroupMetadata {

    private final String groupId;
    private final String memberId;
    private final int memberEpoch;

    public ShareGroupMetadata(String groupId, String memberId, int memberEpoch) {
        this.groupId = Objects.requireNonNull(groupId, "groupId cannot be null");
        this.memberId = Objects.requireNonNull(memberId, "memberId cannot be null");
        this.memberEpoch = memberEpoch;
    }

    public String groupId() {
        return groupId;
    }

    public String memberId() {
        return memberId;
    }

    public int memberEpoch() {
        return memberEpoch;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ShareGroupMetadata that = (ShareGroupMetadata) o;
        return memberEpoch == that.memberEpoch
            && Objects.equals(groupId, that.groupId)
            && Objects.equals(memberId, that.memberId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupId, memberId, memberEpoch);
    }

    @Override
    public String toString() {
        return "ShareGroupMetadata(groupId=" + groupId
            + ", memberId=" + memberId
            + ", memberEpoch=" + memberEpoch
            + ")";
    }
}
