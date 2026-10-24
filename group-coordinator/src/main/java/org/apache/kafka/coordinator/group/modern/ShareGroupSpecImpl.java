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
package org.apache.kafka.coordinator.group.modern;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
import org.apache.kafka.coordinator.group.api.assignor.SubscriptionType;
import org.apache.kafka.coordinator.group.modern.share.ShareGroup.InitMapValue;

import java.util.Map;
import java.util.Objects;

/**
 * The assignment specification for a share group.
 */
public class ShareGroupSpecImpl extends GroupSpecImpl {
    /**
     * The initialized topic partitions, which are the only assignable partitions.
     */
    private final Map<Uuid, InitMapValue> initializedTopics;

    public ShareGroupSpecImpl(
        CoordinatorMetadataImage metadataImage,
        Map<String, MemberSubscriptionAndAssignmentImpl> members,
        SubscriptionType subscriptionType,
        Map<Uuid, InitMapValue> initializedTopics
    ) {
        super(metadataImage, members, subscriptionType);
        this.initializedTopics = Objects.requireNonNull(initializedTopics);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isPartitionAssignable(Uuid topicId, int partitionId) {
        InitMapValue initMapValue = initializedTopics.get(topicId);
        if (initMapValue == null) {
            return false;
        }
        return initMapValue.partitions().contains(partitionId);
    }

    @Override
    public boolean equals(Object o) {
        if (!super.equals(o)) return false;
        ShareGroupSpecImpl groupSpec = (ShareGroupSpecImpl) o;
        return Objects.equals(initializedTopics, groupSpec.initializedTopics);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), initializedTopics);
    }

    @Override
    public String toString() {
        return "ShareGroupSpecImpl(" +
            "members=" + members +
            ", subscriptionType=" + subscriptionType +
            ", initializedTopics=" + initializedTopics +
            ')';
    }
}
