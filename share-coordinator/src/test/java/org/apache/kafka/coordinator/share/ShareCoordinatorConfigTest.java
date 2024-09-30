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

package org.apache.kafka.coordinator.share;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.config.ShareCoordinatorConfig;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ShareCoordinatorConfigTest {

    private static final List<ConfigDef> CONFIG_DEF_LIST = Collections.singletonList(
        ShareCoordinatorConfig.CONFIG_DEF
    );

    public static ShareCoordinatorConfig testConfig() {
        return createConfig(testConfigMap());
    }

    private static Map<String, String> testConfigMapRaw() {
        Map<String, String> configs = new HashMap<>();
        configs.put(ShareCoordinatorConfig.STATE_TOPIC_NUM_PARTITIONS_CONFIG, "1");
        configs.put(ShareCoordinatorConfig.STATE_TOPIC_REPLICATION_FACTOR_CONFIG, "1");
        configs.put(ShareCoordinatorConfig.STATE_TOPIC_MIN_ISR_CONFIG, "1");
        configs.put(ShareCoordinatorConfig.STATE_TOPIC_SEGMENT_BYTES_CONFIG, "1000");
        configs.put(ShareCoordinatorConfig.NUM_THREADS_CONFIG, "1");
        configs.put(ShareCoordinatorConfig.SNAPSHOT_UPDATE_RECORDS_PER_SNAPSHOT_CONFIG, "50");
        configs.put(ShareCoordinatorConfig.WRITE_TIMEOUT_MS_CONFIG, "5000");
        configs.put(ShareCoordinatorConfig.LOAD_BUFFER_SIZE_CONFIG, "555");
        configs.put(ShareCoordinatorConfig.APPEND_LINGER_MS_CONFIG, "10");
        configs.put(ShareCoordinatorConfig.STATE_TOPIC_COMPRESSION_CODEC_CONFIG, String.valueOf(CompressionType.NONE.id));
        return configs;
    }

    public static Map<String, String> testConfigMap() {
        return Collections.unmodifiableMap(testConfigMapRaw());
    }

    public static Map<String, String> testConfigMap(Map<String, String> overrides) {
        Map<String, String> configs = testConfigMapRaw();
        configs.putAll(overrides);
        return Collections.unmodifiableMap(configs);
    }

    public static ShareCoordinatorConfig createConfig(Map<String, String> configs) {
        return new ShareCoordinatorConfig(
            new AbstractConfig(Utils.mergeConfigs(CONFIG_DEF_LIST), configs, false));
    }
}
