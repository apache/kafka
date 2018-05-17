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
package org.apache.kafka.streams.processor;

import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.streams.processor.internals.RecordContext;

/**
 * An interface that allows to dynamically determine the topic name to send to at the sink node of the topology.
 */
@InterfaceStability.Evolving
public interface TopicNameExtractor<K, V> {

    /**
     * Extracts the topic name to send to. The topic name must be pre-existed, since the Kafka Streams library will not
     * try to automatically create the topic with the extracted name, and will fail with a timeout exception if the topic
     * does not exist in the Kafka cluster.
     *
     * @param key           the record key
     * @param value         the record value payload
     * @param recordContext current context metadata of the record
     * @return the topic name to send to
     */
    String extract(K key, V value, RecordContext recordContext);
}
