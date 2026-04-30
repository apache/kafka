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

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;

/**
 * A key for delayed share fetch purgatory that refers to the share partition.
 */
public record DelayedShareFetchGroupKey(String groupId, Uuid topicId, int partition) implements DelayedShareFetchKey {

    public DelayedShareFetchGroupKey(String groupId, TopicIdPartition topicIdPartition) {
        this(groupId, topicIdPartition.topicId(), topicIdPartition.partition());
    }

    @Override
    public String keyLabel() {
        return String.format("groupId=%s, topicId=%s, partition=%s", groupId, topicId, partition);
    }
}
