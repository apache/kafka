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
package org.apache.kafka.connect.runtime;

import org.apache.kafka.connect.util.ConnectorTaskId;

import java.util.Objects;

/**
 * Represents the metadata that is stored as the value of the record that is stored in the
 * {@link org.apache.kafka.connect.storage.StatusBackingStore#put(TopicStatus)},
 */
public class TopicStatus {
    private final String topic;
    private final String connector;
    private final int task;
    private final long discoverTimestamp;

    public TopicStatus(String topic, ConnectorTaskId task, long discoverTimestamp) {
        this(topic, task.connector(), task.task(), discoverTimestamp);
    }

    public TopicStatus(String topic, String connector, int task, long discoverTimestamp) {
        this.topic = Objects.requireNonNull(topic);
        this.connector = Objects.requireNonNull(connector);
        this.task = task;
        this.discoverTimestamp = discoverTimestamp;
    }

    /**
     * Get the name of the topic.
     *
     * @return the topic name; never null
     */
    public String topic() {
        return topic;
    }

    /**
     * Get the name of the connector.
     *
     * @return the connector name; never null
     */
    public String connector() {
        return connector;
    }

    /**
     * Get the ID of the task that stored the topic status.
     *
     * @return the task ID
     */
    public int task() {
        return task;
    }

    /**
     * Get a timestamp that represents when this topic was discovered as being actively used by
     * this connector.
     *
     * @return the discovery timestamp
     */
    public long discoverTimestamp() {
        return discoverTimestamp;
    }

    @Override
    public String toString() {
        return "TopicStatus{" +
                "topic='" + topic + '\'' +
                ", connector='" + connector + '\'' +
                ", task=" + task +
                ", discoverTimestamp=" + discoverTimestamp +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TopicStatus)) {
            return false;
        }
        TopicStatus that = (TopicStatus) o;
        return task == that.task &&
                discoverTimestamp == that.discoverTimestamp &&
                topic.equals(that.topic) &&
                connector.equals(that.connector);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, connector, task, discoverTimestamp);
    }
}
