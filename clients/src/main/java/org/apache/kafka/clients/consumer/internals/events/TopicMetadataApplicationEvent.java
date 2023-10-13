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

import java.util.List;
import java.util.Map;

public class TopicMetadataApplicationEvent extends CompletableApplicationEvent<Map<String, List<PartitionInfo>>> {
    private final String topic;
    public TopicMetadataApplicationEvent(final String topic) {
        super(Type.TOPIC_METADATA);
        this.topic = topic;
    }

    public String topic() {
        return topic;
    }

    @Override
    public String toString() {
        return "TopicMetadataApplicationEvent(topic=" + topic + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TopicMetadataApplicationEvent)) return false;
        if (!super.equals(o)) return false;

        TopicMetadataApplicationEvent that = (TopicMetadataApplicationEvent) o;

        return topic.equals(that.topic);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + topic.hashCode();
        return result;
    }
}
