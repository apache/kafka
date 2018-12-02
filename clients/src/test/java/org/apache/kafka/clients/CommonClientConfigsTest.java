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

package org.apache.kafka.clients;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.junit.Assert.assertEquals;

public class CommonClientConfigsTest {
    private static class TestConfig extends AbstractConfig {
        private static final ConfigDef CONFIG;
        static {
            CONFIG = new ConfigDef()
                .define(CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG,
                    ConfigDef.Type.LONG,
                    50L,
                    atLeast(0L),
                    ConfigDef.Importance.LOW,
                    "")
                .define(CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG,
                    ConfigDef.Type.LONG,
                    1000L,
                    atLeast(0L),
                    ConfigDef.Importance.LOW,
                    "");
        }

        @Override
        protected Map<String, Object> postProcessParsedConfig(final Map<String, Object> parsedValues) {
            return CommonClientConfigs.postProcessReconnectBackoffConfigs(this, parsedValues);
        }

        public TestConfig(Map<?, ?> props) {
            super(CONFIG, props);
        }
    }

    @Test
    public void testExponentialBackoffDefaults() throws Exception {
        TestConfig defaultConf = new TestConfig(Collections.emptyMap());
        assertEquals(Long.valueOf(50L),
                defaultConf.getLong(CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG));
        assertEquals(Long.valueOf(1000L),
                defaultConf.getLong(CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG));

        TestConfig bothSetConfig = new TestConfig(new HashMap<String, Object>() {{
                put(CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG, "123");
                put(CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG, "12345");
            }});
        assertEquals(Long.valueOf(123L),
                bothSetConfig.getLong(CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG));
        assertEquals(Long.valueOf(12345L),
                bothSetConfig.getLong(CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG));

        TestConfig reconnectBackoffSetConf = new TestConfig(new HashMap<String, Object>() {{
                put(CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG, "123");
            }});
        assertEquals(Long.valueOf(123L),
                reconnectBackoffSetConf.getLong(CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG));
        assertEquals(Long.valueOf(123L),
                reconnectBackoffSetConf.getLong(CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG));
    }
}
