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

package org.apache.kafka.server.share;

import org.apache.kafka.common.errors.InvalidConfigurationException;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ShareGroupDynamicConfigTest {

    @Test
    public void testFromPropsInvalid() {
        ShareGroupDynamicConfig.configNames().forEach(name -> {
            if (ShareGroupDynamicConfig.SHARE_RECORD_LOCK_DURATION_MS_CONFIG.equals(name)) {
                assertPropertyInvalid(name, "not_a_number", "-0.1", "1.2");
            } else {
                assertPropertyInvalid(name, "not_a_number", "-1");
            }
        });
    }

    private void assertPropertyInvalid(String name, Object... values) {
        for (Object value : values) {
            Properties props = new Properties();
            props.setProperty(name, value.toString());
            assertThrows(Exception.class, () -> new ShareGroupDynamicConfig(props));
        }
    }

    @Test
    public void testInvalidProps() {
        // Check for invalid recordLockDurationMs, <MIN
        doTestInvalidProps(1000);

        // Check for invalid recordLockDurationMs, >MAX
        doTestInvalidProps(100000);
    }

    private void doTestInvalidProps(int  recordLockDurationMs) {
        Properties props = new Properties();
        props.put(ShareGroupDynamicConfig.SHARE_RECORD_LOCK_DURATION_MS_CONFIG, recordLockDurationMs);
        assertThrows(InvalidConfigurationException.class, () ->
            ShareGroupDynamicConfig.validate(props, createShareGroupConfig()));
    }

    @Test
    public void testFromPropsWithDefaultValue() {
        Map<String, String> defaultValue = new HashMap<>();
        defaultValue.put(ShareGroupDynamicConfig.SHARE_RECORD_LOCK_DURATION_MS_CONFIG, "30000");

        Properties props = new Properties();
        props.put(ShareGroupDynamicConfig.SHARE_RECORD_LOCK_DURATION_MS_CONFIG, "35000");
        ShareGroupDynamicConfig config = ShareGroupDynamicConfig.fromProps(defaultValue, props);

        assertEquals(35000, config.getInt(ShareGroupDynamicConfig.SHARE_RECORD_LOCK_DURATION_MS_CONFIG));
    }

    @Test
    public void testInvalidConfigName() {
        Properties props = new Properties();
        props.put(ShareGroupDynamicConfig.SHARE_RECORD_LOCK_DURATION_MS_CONFIG, "10");
        props.put("invalid.config.name", "10");
        assertThrows(InvalidConfigurationException.class, () ->
            ShareGroupDynamicConfig.validate(props, createShareGroupConfig()));
    }

    private ShareGroupConfig createShareGroupConfig() {
        return ShareGroupConfigTest.createShareGroupConfig(true, 200, 5,
            (short) 10, 30000, 15000, 60000);
    }
}
