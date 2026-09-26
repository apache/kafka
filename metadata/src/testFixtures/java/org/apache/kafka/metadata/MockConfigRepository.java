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
package org.apache.kafka.metadata;

import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfigResource.Type;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class MockConfigRepository implements ConfigRepository {
    private final Map<ConfigResource, Properties> configs = new HashMap<>();

    public static MockConfigRepository forTopic(String topic, String key, String value) {
        Properties properties = new Properties();
        properties.put(key, value);
        return forTopic(topic, properties);
    }

    public static MockConfigRepository forTopic(String topic, Properties properties) {
        MockConfigRepository repository = new MockConfigRepository();
        repository.configs.put(new ConfigResource(Type.TOPIC, topic), properties);
        return repository;
    }

    @Override
    public Properties config(ConfigResource configResource) {
        synchronized (configs) {
            return configs.getOrDefault(configResource, new Properties());
        }
    }

    public void setConfig(ConfigResource configResource, String key, String value) {
        synchronized (configs) {
            Properties properties = configs.getOrDefault(configResource, new Properties());
            Properties newProperties = new Properties();
            newProperties.putAll(properties);
            newProperties.compute(key, (k, v) -> value);
            configs.put(configResource, newProperties);
        }
    }

    public void setTopicConfig(String topicName, String key, String value) {
        synchronized (configs) {
            setConfig(new ConfigResource(Type.TOPIC, topicName), key, value);
        }
    }
}
