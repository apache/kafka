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
import org.apache.kafka.common.utils.Timer;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class TopicMetadataApplicationEvent extends CompletableApplicationEvent<Map<String, List<PartitionInfo>>> {

    private final String topic;
    private final boolean allTopics;

    public TopicMetadataApplicationEvent(final Timer timer) {
        super(Type.TOPIC_METADATA, timer);
        this.topic = null;
        this.allTopics = true;
    }

    public TopicMetadataApplicationEvent(final String topic, final Timer timer) {
        super(Type.TOPIC_METADATA, timer);
        this.topic = topic;
        this.allTopics = false;
    }

    public String topic() {
        return topic;
    }

    public boolean isAllTopics() {
        return allTopics;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), topic, allTopics);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TopicMetadataApplicationEvent)) return false;
        if (!super.equals(o)) return false;

        TopicMetadataApplicationEvent that = (TopicMetadataApplicationEvent) o;

        // TODO: fix this potential NPE
        return topic.equals(that.topic) && (allTopics == that.allTopics);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + " {" + toStringBase() +
                ", topic=" + topic +
                ", allTopics=" + allTopics + "}";
    }
}
