/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common;


import java.util.List;

/**
 * Topic-level configuration holder for a single topic.
 */
public class TopicConfigInfo {

    private final String topic;
    private final List<ConfigEntry> topicConfiguration;

    public TopicConfigInfo(String topic, List<ConfigEntry> topicConfiguration) {
        this.topic = topic;
        this.topicConfiguration = topicConfiguration;
    }

    public String topic() {
        return topic;
    }

    public List<ConfigEntry> topicConfiguration() {
        return topicConfiguration;
    }

    @Override
    public String toString() {
        return "TopicConfigInfo{" +
                "topic='" + topic + '\'' +
                ", topicConfiguration=" + topicConfiguration +
                '}';
    }
}
