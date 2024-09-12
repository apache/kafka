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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.streams.processor.RecordContext;
import org.apache.kafka.streams.processor.TopicNameExtractor;

import java.util.Objects;

/**
 * Static topic name extractor
 */
public class StaticTopicNameExtractor<K, V> implements TopicNameExtractor<K, V> {

    public final String topicName;

    public StaticTopicNameExtractor(final String topicName) {
        this.topicName = topicName;
    }

    public String extract(final K key, final V value, final RecordContext recordContext) {
        return topicName;
    }

    @Override
    public String toString() {
        return "StaticTopicNameExtractor(" + topicName + ")";
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final StaticTopicNameExtractor<?, ?> that = (StaticTopicNameExtractor<?, ?>) o;
        return Objects.equals(topicName, that.topicName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topicName);
    }
}
