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
package org.apache.kafka.test;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

public class StreamsTestUtils {

    public static Properties getStreamsConfig(final String applicationId,
                                              final String bootstrapServers,
                                              final String keySerdeClassName,
                                              final String valueSerdeClassName,
                                              final Properties additional) {

        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamsConfiguration.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "1000");
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, keySerdeClassName);
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, valueSerdeClassName);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        streamsConfiguration.putAll(additional);
        return streamsConfiguration;

    }

    public static Properties minimalStreamsConfig() {
        final Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString());
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "anyserver:9092");
        return properties;
    }

    public static <K, V> List<KeyValue<K, V>> toList(final Iterator<KeyValue<K, V>> iterator) {
        final List<KeyValue<K, V>> results = new ArrayList<>();

        while (iterator.hasNext()) {
            results.add(iterator.next());
        }
        return results;
    }
}
