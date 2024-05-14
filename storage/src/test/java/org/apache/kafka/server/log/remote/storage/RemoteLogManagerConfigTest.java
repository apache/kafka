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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig.DEFAULT_REMOTE_LOG_METADATA_MANAGER_CLASS_NAME;
import static org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP;

public class RemoteLogManagerConfigTest {

    private static class TestConfig extends AbstractConfig {
        public TestConfig(Map<?, ?> originals) {
            super(RemoteLogManagerConfig.CONFIG_DEF, originals, true);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testValidConfigs(boolean useDefaultRemoteLogMetadataManagerClass) {
        String rsmPrefix = "__custom.rsm.";
        String rlmmPrefix = "__custom.rlmm.";
        Map<String, Object> rsmProps = Collections.singletonMap("rsm.prop", "val");
        Map<String, Object> rlmmProps = Collections.singletonMap("rlmm.prop", "val");
        String remoteLogMetadataManagerClass = useDefaultRemoteLogMetadataManagerClass ? DEFAULT_REMOTE_LOG_METADATA_MANAGER_CLASS_NAME : "dummy.remote.log.metadata.class";
        RemoteLogManagerConfig expectedRemoteLogManagerConfig
                = new RemoteLogManagerConfig(true, "dummy.remote.storage.class", "dummy.remote.storage.class.path",
                                             remoteLogMetadataManagerClass, "dummy.remote.log.metadata.class.path",
                                             "listener.name", 1024 * 1024L, 1, 60000L, 100L, 60000L, 0.3, 10, 100, 100,
                                             rsmPrefix, rsmProps, rlmmPrefix, rlmmProps);

        Map<String, Object> props = extractProps(expectedRemoteLogManagerConfig);
        rsmProps.forEach((k, v) -> props.put(rsmPrefix + k, v));
        rlmmProps.forEach((k, v) -> props.put(rlmmPrefix + k, v));
        // Removing remote.log.metadata.manager.class.name so that the default value gets picked up.
        if (useDefaultRemoteLogMetadataManagerClass) {
            props.remove(REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP);
        }
        TestConfig config = new TestConfig(props);
        RemoteLogManagerConfig remoteLogManagerConfig = new RemoteLogManagerConfig(config);
        Assertions.assertEquals(expectedRemoteLogManagerConfig, remoteLogManagerConfig);
    }

    private Map<String, Object> extractProps(RemoteLogManagerConfig remoteLogManagerConfig) {
        Map<String, Object> props = new HashMap<>();
        props.put(RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP,
                  remoteLogManagerConfig.enableRemoteStorageSystem());
        props.put(RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CLASS_NAME_PROP,
                  remoteLogManagerConfig.remoteStorageManagerClassName());
        props.put(RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CLASS_PATH_PROP,
                  remoteLogManagerConfig.remoteStorageManagerClassPath());
        props.put(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP,
                  remoteLogManagerConfig.remoteLogMetadataManagerClassName());
        props.put(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CLASS_PATH_PROP,
                  remoteLogManagerConfig.remoteLogMetadataManagerClassPath());
        props.put(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_LISTENER_NAME_PROP,
                  remoteLogManagerConfig.remoteLogMetadataManagerListenerName());
        props.put(RemoteLogManagerConfig.REMOTE_LOG_INDEX_FILE_CACHE_TOTAL_SIZE_BYTES_PROP,
                  remoteLogManagerConfig.remoteLogIndexFileCacheTotalSizeBytes());
        props.put(RemoteLogManagerConfig.REMOTE_LOG_MANAGER_THREAD_POOL_SIZE_PROP,
                  remoteLogManagerConfig.remoteLogManagerThreadPoolSize());
        props.put(RemoteLogManagerConfig.REMOTE_LOG_MANAGER_TASK_INTERVAL_MS_PROP,
                  remoteLogManagerConfig.remoteLogManagerTaskIntervalMs());
        props.put(RemoteLogManagerConfig.REMOTE_LOG_MANAGER_TASK_RETRY_BACK_OFF_MS_PROP,
                  remoteLogManagerConfig.remoteLogManagerTaskRetryBackoffMs());
        props.put(RemoteLogManagerConfig.REMOTE_LOG_MANAGER_TASK_RETRY_BACK_OFF_MAX_MS_PROP,
                  remoteLogManagerConfig.remoteLogManagerTaskRetryBackoffMaxMs());
        props.put(RemoteLogManagerConfig.REMOTE_LOG_MANAGER_TASK_RETRY_JITTER_PROP,
                  remoteLogManagerConfig.remoteLogManagerTaskRetryJitter());
        props.put(RemoteLogManagerConfig.REMOTE_LOG_READER_THREADS_PROP,
                  remoteLogManagerConfig.remoteLogReaderThreads());
        props.put(RemoteLogManagerConfig.REMOTE_LOG_READER_MAX_PENDING_TASKS_PROP,
                  remoteLogManagerConfig.remoteLogReaderMaxPendingTasks());
        props.put(RemoteLogManagerConfig.REMOTE_LOG_METADATA_CUSTOM_METADATA_MAX_BYTES_PROP,
                  remoteLogManagerConfig.remoteLogMetadataCustomMetadataMaxBytes());
        props.put(RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CONFIG_PREFIX_PROP,
                  remoteLogManagerConfig.remoteStorageManagerPrefix());
        props.put(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CONFIG_PREFIX_PROP,
                  remoteLogManagerConfig.remoteLogMetadataManagerPrefix());
        return props;
    }
}
