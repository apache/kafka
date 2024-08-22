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
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.config.ShareCoordinatorConfig;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Type.INT;

public class ShareCoordinatorConfigTest {
    // in production - these configs will be defined in group coordinator
    // for share coordinator unit tests, we are adding these here to not add
    // dependency on group coord module.
    private static final ConfigDef ADDITIONAL_CONFIG = new ConfigDef()
        .define("offsets.commit.timeout.ms", INT, 5000, atLeast(1), HIGH, "commit timeout")
        .define("offsets.load.buffer.size", INT, 5 * 1024 * 1024, atLeast(1), HIGH, "load buffer");

    private static final List<ConfigDef> CONFIG_DEF_LIST = Arrays.asList(
        ShareCoordinatorConfig.CONFIG_DEF,
        ADDITIONAL_CONFIG
    );

    public static ShareCoordinatorConfig testConfig() {
        return createConfig(testConfigMap());
    }

    public static Map<String, String> testConfigMap() {
        Map<String, String> configs = new HashMap<>();
        configs.put(ShareCoordinatorConfig.STATE_TOPIC_NUM_PARTITIONS_CONFIG, "1");
        configs.put(ShareCoordinatorConfig.STATE_TOPIC_REPLICATION_FACTOR_CONFIG, "1");
        configs.put(ShareCoordinatorConfig.STATE_TOPIC_MIN_ISR_CONFIG, "1");
        configs.put(ShareCoordinatorConfig.STATE_TOPIC_SEGMENT_BYTES_CONFIG, "1000");
        configs.put(ShareCoordinatorConfig.NUM_THREADS_CONFIG, "1");
        configs.put(ShareCoordinatorConfig.SNAPSHOT_UPDATE_RECORDS_PER_SNAPSHOT_CONFIG, "50");
        configs.put("offsets.commit.timeout.ms", "5000");
        configs.put("offsets.load.buffer.size", "555");
        return Collections.unmodifiableMap(configs);
    }

    public static ShareCoordinatorConfig createConfig(Map<String, String> configs) {
        return new ShareCoordinatorConfig(
            new AbstractConfig(Utils.mergeConfigs(CONFIG_DEF_LIST), configs, false));
    }
}
