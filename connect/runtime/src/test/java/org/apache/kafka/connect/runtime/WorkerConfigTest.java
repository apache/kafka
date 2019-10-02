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
package org.apache.kafka.connect.runtime;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class WorkerConfigTest {

    @Test
    public void testAdminListenersConfigAllowedValues() {
        Map<String, String> props = baseProps();

        // no value set for "admin.listeners"
        WorkerConfig config = new WorkerConfig(WorkerConfig.baseConfigDef(), props);
        assertNull("Default value should be null.", config.getList(WorkerConfig.ADMIN_LISTENERS_CONFIG));

        props.put(WorkerConfig.ADMIN_LISTENERS_CONFIG, "");
        config = new WorkerConfig(WorkerConfig.baseConfigDef(), props);
        assertTrue(config.getList(WorkerConfig.ADMIN_LISTENERS_CONFIG).isEmpty());

        props.put(WorkerConfig.ADMIN_LISTENERS_CONFIG, "http://a.b:9999, https://a.b:7812");
        config = new WorkerConfig(WorkerConfig.baseConfigDef(), props);
        assertEquals(config.getList(WorkerConfig.ADMIN_LISTENERS_CONFIG), Arrays.asList("http://a.b:9999", "https://a.b:7812"));

        new WorkerConfig(WorkerConfig.baseConfigDef(), props);
    }

    @Test(expected = ConfigException.class)
    public void testAdminListenersNotAllowingEmptyStrings() {
        Map<String, String> props = baseProps();
        props.put(WorkerConfig.ADMIN_LISTENERS_CONFIG, "http://a.b:9999,");
        new WorkerConfig(WorkerConfig.baseConfigDef(), props);
    }

    @Test(expected = ConfigException.class)
    public void testAdminListenersNotAllowingBlankStrings() {
        Map<String, String> props = baseProps();
        props.put(WorkerConfig.ADMIN_LISTENERS_CONFIG, "http://a.b:9999, ,https://a.b:9999");
        new WorkerConfig(WorkerConfig.baseConfigDef(), props);
    }

    private Map<String, String> baseProps() {
        Map<String, String> props = new HashMap<>();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        props.put(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        return props;
    }

}
