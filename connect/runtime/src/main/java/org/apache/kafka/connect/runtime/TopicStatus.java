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

public class TopicStatus {
    private final String topic;
    private final String connector;
    private final int task;
    private final long discoverTimestamp;

    public TopicStatus(String topic, ConnectorTaskId task, long discoverTimestamp) {
        //TODO: check non-null
        this.topic = topic;
        this.connector = task.connector();
        this.task = task.task();
        this.discoverTimestamp = discoverTimestamp;
    }

    public TopicStatus(String topic, String connector, int task, long discoverTimestamp) {
        this.topic = topic;
        this.connector = connector;
        this.task = task;
        this.discoverTimestamp = discoverTimestamp;
    }

    public String topic() {
        return topic;
    }

    public String connector() {
        return connector;
    }

    public int task() {
        return task;
    }

    public long discoverTimestamp() {
        return discoverTimestamp;
    }
}
