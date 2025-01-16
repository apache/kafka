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
package org.apache.kafka.server.log.remote.storage;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigException;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class RemoteLogManagerConfigTest {
    @Test
    public void testValidConfigs() {
        String rsmPrefix = "__custom.rsm.";
        String rlmmPrefix = "__custom.rlmm.";
        Map<String, Object> rsmProps = Collections.singletonMap("rsm.prop", "val");
        Map<String, Object> rlmmProps = Collections.singletonMap("rlmm.prop", "val");

        Map<String, Object> props = getRLMProps(rsmPrefix, rlmmPrefix);
        rsmProps.forEach((k, v) -> props.put(rsmPrefix + k, v));
        rlmmProps.forEach((k, v) -> props.put(rlmmPrefix + k, v));
        RLMTestConfig config = new RLMTestConfig(props);

        RemoteLogManagerConfig rlmConfig = config.remoteLogManagerConfig();
        assertEquals(rsmProps, rlmConfig.remoteStorageManagerProps());
        assertEquals(rlmmProps, rlmConfig.remoteLogMetadataManagerProps());
    }

    @Test
    public void testDefaultConfigs() {
        // Even with empty properties, RemoteLogManagerConfig has default values
        Map<String, Object> emptyProps = new HashMap<>();
        RemoteLogManagerConfig remoteLogManagerConfigEmptyConfig = new RLMTestConfig(emptyProps).remoteLogManagerConfig();
        assertEquals(RemoteLogManagerConfig.DEFAULT_REMOTE_LOG_MANAGER_THREAD_POOL_SIZE, remoteLogManagerConfigEmptyConfig.remoteLogManagerThreadPoolSize());
        assertEquals(RemoteLogManagerConfig.DEFAULT_REMOTE_LOG_MANAGER_COPY_QUOTA_WINDOW_NUM, remoteLogManagerConfigEmptyConfig.remoteLogManagerCopyNumQuotaSamples());
    }

    @Test
    public void testThreadPoolDefaults() {
        // Even with empty properties, RemoteLogManagerConfig has default values
        Map<String, Object> emptyProps = new HashMap<>();
        RemoteLogManagerConfig remoteLogManagerConfigEmptyConfig = new RLMTestConfig(emptyProps).remoteLogManagerConfig();
        assertEquals(RemoteLogManagerConfig.DEFAULT_REMOTE_LOG_MANAGER_THREAD_POOL_SIZE, remoteLogManagerConfigEmptyConfig.remoteLogManagerThreadPoolSize());
        assertEquals(RemoteLogManagerConfig.DEFAULT_REMOTE_LOG_MANAGER_COPIER_THREAD_POOL_SIZE, remoteLogManagerConfigEmptyConfig.remoteLogManagerCopierThreadPoolSize());
        assertEquals(RemoteLogManagerConfig.DEFAULT_REMOTE_LOG_MANAGER_EXPIRATION_THREAD_POOL_SIZE, remoteLogManagerConfigEmptyConfig.remoteLogManagerExpirationThreadPoolSize());
    }

    @Test
    public void testValidateEmptyStringConfig() {
        // Test with a empty string props should throw ConfigException
        Map<String, Object> emptyStringProps = Collections.singletonMap(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_LISTENER_NAME_PROP, "");
        assertThrows(ConfigException.class, () ->
                new RLMTestConfig(emptyStringProps).remoteLogManagerConfig());
    }

    private Map<String, Object> getRLMProps(String rsmPrefix, String rlmmPrefix) {
        Map<String, Object> props = new HashMap<>();
        props.put(RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, true);
        props.put(RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CLASS_NAME_PROP,
                "dummy.remote.storage.class");
        props.put(RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CLASS_PATH_PROP,
                "dummy.remote.storage.class.path");
        props.put(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP,
                RemoteLogManagerConfig.DEFAULT_REMOTE_LOG_METADATA_MANAGER_CLASS_NAME);
        props.put(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CLASS_PATH_PROP,
                "dummy.remote.log.metadata.class.path");
        props.put(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_LISTENER_NAME_PROP,
                "listener.name");
        props.put(RemoteLogManagerConfig.REMOTE_LOG_INDEX_FILE_CACHE_TOTAL_SIZE_BYTES_PROP,
                1024 * 1024L);
        props.put(RemoteLogManagerConfig.REMOTE_LOG_MANAGER_THREAD_POOL_SIZE_PROP,
                1);
        props.put(RemoteLogManagerConfig.REMOTE_LOG_MANAGER_COPIER_THREAD_POOL_SIZE_PROP,
                1);
        props.put(RemoteLogManagerConfig.REMOTE_LOG_MANAGER_EXPIRATION_THREAD_POOL_SIZE_PROP,
                1);
        props.put(RemoteLogManagerConfig.REMOTE_LOG_MANAGER_TASK_INTERVAL_MS_PROP,
                60000L);
        props.put(RemoteLogManagerConfig.REMOTE_LOG_MANAGER_TASK_RETRY_BACK_OFF_MS_PROP,
                100L);
        props.put(RemoteLogManagerConfig.REMOTE_LOG_MANAGER_TASK_RETRY_BACK_OFF_MAX_MS_PROP,
                60000L);
        props.put(RemoteLogManagerConfig.REMOTE_LOG_MANAGER_TASK_RETRY_JITTER_PROP,
                0.3);
        props.put(RemoteLogManagerConfig.REMOTE_LOG_READER_THREADS_PROP,
                2);
        props.put(RemoteLogManagerConfig.REMOTE_LOG_READER_MAX_PENDING_TASKS_PROP,
                100);
        props.put(RemoteLogManagerConfig.REMOTE_LOG_METADATA_CUSTOM_METADATA_MAX_BYTES_PROP,
                100);
        props.put(RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CONFIG_PREFIX_PROP,
                rsmPrefix);
        props.put(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CONFIG_PREFIX_PROP,
                rlmmPrefix);
        return props;
    }

    private static class RLMTestConfig extends AbstractConfig {
        private final RemoteLogManagerConfig rlmConfig;

        public RLMTestConfig(Map<?, ?> originals) {
            super(RemoteLogManagerConfig.configDef(), originals, true);
            rlmConfig = new RemoteLogManagerConfig(this);
        }

        public RemoteLogManagerConfig remoteLogManagerConfig() {
            return rlmConfig;
        }
    }
}
