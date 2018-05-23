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

package org.apache.kafka.connect.cli;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class CliUtilTest {

    private static Map<String, String> config(String workerId) {
        Map<String, String> map = new HashMap<>();
        map.put(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        map.put(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        map.put(WorkerConfig.INTERNAL_KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        map.put(WorkerConfig.INTERNAL_VALUE_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        map.put(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, "CliUtilTest.tempfile");
        map.put(DistributedConfig.GROUP_ID_CONFIG, "CliUtilTest.id");
        map.put(DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG, "CliUtilTest.offset.storage");
        map.put(DistributedConfig.CONFIG_TOPIC_CONFIG, "CliUtilTest.config.storage");
        map.put(DistributedConfig.STATUS_STORAGE_TOPIC_CONFIG, "CliUtilTest.status.storage");
        if (workerId != null) map.put(WorkerConfig.WORKER_ID_CONFIG, workerId);
        return map;
    }

    @Test
    public void testWorkerID() {
        List<WorkerConfig> configs = Arrays.asList(new StandaloneConfig(config(null)), new DistributedConfig(config(null)));
        for (WorkerConfig config : configs) {
            Assert.assertNull(CliUtil.workerId(config));
        }

        String workerId = "worker_id";
        Map<String, String> map = config(workerId);
        configs = Arrays.asList(new StandaloneConfig(map), new DistributedConfig(map));
        for (WorkerConfig config : configs) {
            Assert.assertEquals(workerId, CliUtil.workerId(config));
        }
    }
}
