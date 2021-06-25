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
import java.util.List;

import static org.apache.kafka.connect.runtime.WorkerConfig.LISTENERS_DEFAULT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertThrows;

public class WorkerConfigTest {
    private static final List<String> VALID_HEADER_CONFIGS = Arrays.asList(
            "add \t Cache-Control: no-cache, no-store, must-revalidate",
            "add \r X-XSS-Protection: 1; mode=block",
            "\n add Strict-Transport-Security: max-age=31536000; includeSubDomains",
            "AdD   Strict-Transport-Security:  \r  max-age=31536000;  includeSubDomains",
            "AdD \t Strict-Transport-Security : \n   max-age=31536000;  includeSubDomains",
            "add X-Content-Type-Options: \r nosniff",
            "Set \t X-Frame-Options: \t Deny\n ",
            "seT \t X-Cache-Info: \t not cacheable\n ",
            "seTDate \t Expires: \r 31540000000",
            "adDdate \n Last-Modified: \t 0"
    );

    private static final List<String> INVALID_HEADER_CONFIGS = Arrays.asList(
            "set \t",
            "badaction \t X-Frame-Options:DENY",
            "set add X-XSS-Protection:1",
            "addX-XSS-Protection",
            "X-XSS-Protection:",
            "add set X-XSS-Protection: 1",
            "add X-XSS-Protection:1 X-XSS-Protection:1 ",
            "add X-XSS-Protection",
            "set X-Frame-Options:DENY, add  :no-cache, no-store, must-revalidate "
    );

    @Test
    public void testListenersConfigAllowedValues() {
        Map<String, String> props = baseProps();

        // no value set for "listeners"
        WorkerConfig config = new WorkerConfig(WorkerConfig.baseConfigDef(), props);
        assertEquals(LISTENERS_DEFAULT, config.getList(WorkerConfig.LISTENERS_CONFIG));

        props.put(WorkerConfig.LISTENERS_CONFIG, "http://a.b:9999");
        config = new WorkerConfig(WorkerConfig.baseConfigDef(), props);
        assertEquals(Arrays.asList("http://a.b:9999"), config.getList(WorkerConfig.LISTENERS_CONFIG));

        props.put(WorkerConfig.LISTENERS_CONFIG, "http://a.b:9999, https://a.b:7812");
        config = new WorkerConfig(WorkerConfig.baseConfigDef(), props);
        assertEquals(Arrays.asList("http://a.b:9999", "https://a.b:7812"), config.getList(WorkerConfig.LISTENERS_CONFIG));

        new WorkerConfig(WorkerConfig.baseConfigDef(), props);
    }

    @Test
    public void testListenersConfigNotAllowedValues() {
        Map<String, String> props = baseProps();
        assertEquals(LISTENERS_DEFAULT, new WorkerConfig(WorkerConfig.baseConfigDef(), props).getList(WorkerConfig.LISTENERS_CONFIG));

        props.put(WorkerConfig.LISTENERS_CONFIG, "");
        ConfigException ce = assertThrows(ConfigException.class, () -> new WorkerConfig(WorkerConfig.baseConfigDef(), props));
        assertTrue(ce.getMessage().contains(" listeners"));

        props.put(WorkerConfig.LISTENERS_CONFIG, ",,,");
        ce = assertThrows(ConfigException.class, () -> new WorkerConfig(WorkerConfig.baseConfigDef(), props));
        assertTrue(ce.getMessage().contains(" listeners"));

        props.put(WorkerConfig.LISTENERS_CONFIG, "http://a.b:9999,");
        ce = assertThrows(ConfigException.class, () -> new WorkerConfig(WorkerConfig.baseConfigDef(), props));
        assertTrue(ce.getMessage().contains(" listeners"));

        props.put(WorkerConfig.LISTENERS_CONFIG, "http://a.b:9999, ,https://a.b:9999");
        ce = assertThrows(ConfigException.class, () -> new WorkerConfig(WorkerConfig.baseConfigDef(), props));
        assertTrue(ce.getMessage().contains(" listeners"));
    }

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
        assertEquals(Arrays.asList("http://a.b:9999", "https://a.b:7812"), config.getList(WorkerConfig.ADMIN_LISTENERS_CONFIG));

        new WorkerConfig(WorkerConfig.baseConfigDef(), props);
    }

    @Test
    public void testAdminListenersNotAllowingEmptyStrings() {
        Map<String, String> props = baseProps();

        props.put(WorkerConfig.ADMIN_LISTENERS_CONFIG, "http://a.b:9999,");
        ConfigException ce = assertThrows(ConfigException.class, () -> new WorkerConfig(WorkerConfig.baseConfigDef(), props));
        assertTrue(ce.getMessage().contains(" admin.listeners"));
    }

    @Test
    public void testAdminListenersNotAllowingBlankStrings() {
        Map<String, String> props = baseProps();
        props.put(WorkerConfig.ADMIN_LISTENERS_CONFIG, "http://a.b:9999, ,https://a.b:9999");
        assertThrows(ConfigException.class, () -> new WorkerConfig(WorkerConfig.baseConfigDef(), props));
    }

    @Test
    public void testInvalidHeaderConfigs() {
        for (String config : INVALID_HEADER_CONFIGS) {
            assertInvalidHeaderConfig(config);
        }
    }

    @Test
    public void testValidHeaderConfigs() {
        for (String config : VALID_HEADER_CONFIGS) {
            assertValidHeaderConfig(config);
        }
    }

    private void assertInvalidHeaderConfig(String config) {
        assertThrows(ConfigException.class, () -> WorkerConfig.validateHttpResponseHeaderConfig(config));
    }

    private void assertValidHeaderConfig(String config) {
        WorkerConfig.validateHttpResponseHeaderConfig(config);
    }

    private Map<String, String> baseProps() {
        Map<String, String> props = new HashMap<>();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        props.put(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        return props;
    }

}
