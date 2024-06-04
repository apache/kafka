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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig.DEFAULT_REMOTE_LOG_METADATA_MANAGER_CLASS_NAME;
import static org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP;
import static org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_LISTENER_NAME_PROP;
import static org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP;
import static org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CLASS_PATH_PROP;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class RemoteLogManagerConfigTest {

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testValidConfigs(boolean useDefaultRemoteLogMetadataManagerClass) {
        String rsmPrefix = "__custom.rsm.";
        String rlmmPrefix = "__custom.rlmm.";
        Map<String, Object> rsmProps = Collections.singletonMap("rsm.prop", "val");
        Map<String, Object> rlmmProps = Collections.singletonMap("rlmm.prop", "val");

        Map<String, Object> props = getRLMProps(useDefaultRemoteLogMetadataManagerClass, rsmProps, rlmmProps);
        rsmProps.forEach((k, v) -> props.put(rsmPrefix + k, v));
        rlmmProps.forEach((k, v) -> props.put(rlmmPrefix + k, v));

        RemoteLogManagerConfig expectedRemoteLogManagerConfig = new RemoteLogManagerConfig(props);

        // Removing remote.log.metadata.manager.class.name so that the default value gets picked up.
        if (useDefaultRemoteLogMetadataManagerClass) {
            props.remove(REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP);
        }
        RemoteLogManagerConfig remoteLogManagerConfig = new RemoteLogManagerConfig(props);
        assertEquals(expectedRemoteLogManagerConfig.values(), remoteLogManagerConfig.values());
    }

    private Map<String, Object> getRLMProps(boolean useDefaultRemoteLogMetadataManagerClass, Map<String, Object> rsmProps, Map<String, Object> rlmmProps) {
        String remoteLogMetadataManagerClass = useDefaultRemoteLogMetadataManagerClass ? DEFAULT_REMOTE_LOG_METADATA_MANAGER_CLASS_NAME : "dummy.remote.log.metadata.class";

        Map<String, Object> props = new HashMap<>();
        props.put(REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, true);
        props.put(RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CLASS_NAME_PROP,
                "dummy.remote.storage.class");
        props.put(REMOTE_STORAGE_MANAGER_CLASS_PATH_PROP,
                "dummy.remote.storage.class.path");
        props.put(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP,
                remoteLogMetadataManagerClass);
        props.put(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CLASS_PATH_PROP,
                "dummy.remote.log.metadata.class.path");
        props.put(REMOTE_LOG_METADATA_MANAGER_LISTENER_NAME_PROP,
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
                10);
        props.put(RemoteLogManagerConfig.REMOTE_LOG_READER_MAX_PENDING_TASKS_PROP,
                100);
        props.put(RemoteLogManagerConfig.REMOTE_LOG_METADATA_CUSTOM_METADATA_MAX_BYTES_PROP,
                100);
        props.put(RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CONFIG_PREFIX_PROP,
                rsmProps);
        props.put(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CONFIG_PREFIX_PROP,
                rlmmProps);
        return props;
    }
}