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

import static org.apache.kafka.clients.consumer.internals.events.ApplicationEventType.TOPIC_METADATA;

public class TopicMetadataApplicationEvent extends CompletableApplicationEvent<Map<String, List<PartitionInfo>>> {

    private final String topic;
    private final boolean allTopics;

    public TopicMetadataApplicationEvent(final Timer timer) {
        super(TOPIC_METADATA, timer);
        this.topic = null;
        this.allTopics = true;
    }

    public TopicMetadataApplicationEvent(final String topic, final Timer timer) {
        super(TOPIC_METADATA, timer);
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
    public String toStringBase() {
        return super.toStringBase() + ", topic=" + topic + ", allTopics=" + allTopics;
    }
}
