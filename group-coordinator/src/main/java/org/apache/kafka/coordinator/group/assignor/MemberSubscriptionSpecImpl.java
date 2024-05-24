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
package org.apache.kafka.coordinator.group.assignor;

import org.apache.kafka.common.Uuid;

import java.util.Optional;
import java.util.Set;

/**
 * Implementation of the {@link MemberSubscriptionSpec} interface.
 */
public class MemberSubscriptionSpecImpl implements MemberSubscriptionSpec {
    private final Optional<String> rackId;
    private final Set<Uuid> subscribedTopicIds;

    /**
     * Constructs a new {@code MemberSubscriptionSpecImpl}.
     *
     * @param rackId                The rack Id.
     * @param subscribedTopicIds    The set of subscribed topic Ids.
     */
    public MemberSubscriptionSpecImpl(
        Optional<String> rackId,
        Set<Uuid> subscribedTopicIds
    ) {
        this.rackId = rackId;
        this.subscribedTopicIds = subscribedTopicIds;
    }

    @Override
    public Optional<String> rackId() {
        return rackId;
    }

    @Override
    public Set<Uuid> subscribedTopicIds() {
        return subscribedTopicIds;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MemberSubscriptionSpecImpl that = (MemberSubscriptionSpecImpl) o;
        return rackId.equals(that.rackId) &&
            subscribedTopicIds.equals(that.subscribedTopicIds);
    }

    @Override
    public int hashCode() {
        int result = rackId.hashCode();
        result = 31 * result + subscribedTopicIds.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "MemberSubscriptionSpecImpl(rackId=" + rackId.orElse("N/A") +
            ", subscribedTopicIds=" + subscribedTopicIds +
            ')';
    }
}
