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

package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.common.Uuid;

import java.util.Objects;

/**
 * A topic represented by a universally unique identifier and a topic name.
 */
public class Topic {

    private final Uuid topicId;

    private final String topicName;

    public Topic(Uuid topicId) {
        this(topicId, null);
    }

    public Topic(String topicName) {
        this(null, topicName);
    }

    public Topic(Uuid topicId, String topicName) {
        this.topicId = topicId;
        this.topicName = topicName;
    }

    public Uuid topicId() {
        return topicId;
    }

    public String topicName() {
        return topicName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Topic other = (Topic) o;
        return Objects.equals(topicId, other.topicId) && Objects.equals(topicName, other.topicName);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = prime + topicId.hashCode();
        result = prime * result + topicName.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return topicId + ":" + topicName;
    }
}
