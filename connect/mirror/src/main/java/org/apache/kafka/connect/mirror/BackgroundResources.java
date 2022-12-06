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
package org.apache.kafka.connect.mirror;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.Utils;

import java.time.Duration;
import java.util.IdentityHashMap;
import java.util.Map;

import static org.apache.kafka.connect.mirror.MirrorConnectorConfig.FORWARDING_ADMIN_CLASS;

public class BackgroundResources implements AutoCloseable {

    private final Map<AutoCloseable, String> resources;

    public BackgroundResources() {
        resources = new IdentityHashMap<>();
    }

    private <T extends AutoCloseable> T open(T closeable, String name) {
        resources.put(closeable, name);
        return closeable;
    }

    @Override
    public void close() {
        for (Map.Entry<AutoCloseable, String> entry : resources.entrySet()) {
            Utils.closeQuietly(entry.getKey(), entry.getValue());
        }
    }

    public KafkaConsumer<byte[], byte[]> consumer(Map<String, Object> props, String name) {
        return open(new KafkaConsumer<>(props, new ByteArrayDeserializer(), new ByteArrayDeserializer()), name);
    }

    public KafkaProducer<byte[], byte[]> producer(Map<String, Object> props, String name) {
        return open(new KafkaProducer<>(props, new ByteArraySerializer(), new ByteArraySerializer()), name);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public Admin admin(MirrorConnectorConfig config, Map<String, Object> props, String name) {
        try {
            return open(Utils.newParameterizedInstance(
                    config.getClass(FORWARDING_ADMIN_CLASS).getName(), (Class<Map<String, Object>>) (Class) Map.class, props
            ), name);
        } catch (ClassNotFoundException e) {
            throw new KafkaException("Can't create instance of " + config.getString(FORWARDING_ADMIN_CLASS), e);
        }
    }

    public TopicFilter topicFilter(MirrorConnectorConfig config, String name) {
        return open(config.getConfiguredInstance(MirrorConnectorConfig.TOPIC_FILTER_CLASS, TopicFilter.class), name);
    }

    public GroupFilter groupFilter(MirrorCheckpointConfig config, String name) {
        return open(config.getConfiguredInstance(MirrorCheckpointConfig.GROUP_FILTER_CLASS, GroupFilter.class), name);
    }

    public OffsetSyncStore offsetSyncStore(MirrorCheckpointTaskConfig config, String name) {
        return open(new OffsetSyncStore(config), name);
    }

    public Scheduler scheduler(Class<?> clazz, Duration timeout, String name) {
        return open(new Scheduler(clazz, timeout), name);
    }

    public ConfigPropertyFilter configPropertyFilter(MirrorSourceConfig config, String name) {
        return open(config.getConfiguredInstance(MirrorSourceConfig.CONFIG_PROPERTY_FILTER_CLASS, ConfigPropertyFilter.class), name);
    }

    public MirrorSourceMetrics sourceMetrics(MirrorSourceTaskConfig config, String name) {
        MirrorSourceMetrics metrics = new MirrorSourceMetrics(config);
        config.metricsReporters().forEach(metrics::addReporter);
        return open(metrics, name);
    }

    public MirrorCheckpointMetrics checkpointMetrics(MirrorCheckpointTaskConfig config, String name) {
        MirrorCheckpointMetrics metrics = new MirrorCheckpointMetrics(config);
        config.metricsReporters().forEach(metrics::addReporter);
        return open(metrics, name);
    }
}
