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
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.ValidString.in;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CommonClientConfigsTest {
    @SuppressWarnings("deprecation")
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
                    "")
                .define(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                    ConfigDef.Type.STRING,
                    CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL,
                    in(Utils.enumOptions(SecurityProtocol.class)),
                    ConfigDef.Importance.MEDIUM,
                    CommonClientConfigs.SECURITY_PROTOCOL_DOC)
                .define(SaslConfigs.SASL_MECHANISM,
                    ConfigDef.Type.STRING,
                    SaslConfigs.DEFAULT_SASL_MECHANISM,
                    ConfigDef.Importance.MEDIUM,
                    SaslConfigs.SASL_MECHANISM_DOC)
                .define(CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG,
                    ConfigDef.Type.LIST,
                    Collections.emptyList(),
                    new ConfigDef.NonNullValidator(),
                    ConfigDef.Importance.LOW,
                    CommonClientConfigs.METRIC_REPORTER_CLASSES_DOC)
                .define(CommonClientConfigs.AUTO_INCLUDE_JMX_REPORTER_CONFIG,
                    ConfigDef.Type.BOOLEAN,
                    true,
                    ConfigDef.Importance.LOW,
                    CommonClientConfigs.AUTO_INCLUDE_JMX_REPORTER_DOC);
        }

        @Override
        protected Map<String, Object> postProcessParsedConfig(final Map<String, Object> parsedValues) {
            CommonClientConfigs.postValidateSaslMechanismConfig(this);
            return CommonClientConfigs.postProcessReconnectBackoffConfigs(this, parsedValues);
        }

        public TestConfig(Map<?, ?> props) {
            super(CONFIG, props);
        }
    }

    @Test
    public void testExponentialBackoffDefaults() {
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

    @Test
    public void testInvalidSaslMechanism() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name);
        configs.put(SaslConfigs.SASL_MECHANISM, null);
        ConfigException ce = assertThrows(ConfigException.class, () -> new TestConfig(configs));
        assertTrue(ce.getMessage().contains(SaslConfigs.SASL_MECHANISM));

        configs.put(SaslConfigs.SASL_MECHANISM, "");
        ce = assertThrows(ConfigException.class, () -> new TestConfig(configs));
        assertTrue(ce.getMessage().contains(SaslConfigs.SASL_MECHANISM));
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testMetricsReporters() {
        TestConfig config = new TestConfig(Collections.emptyMap());
        List<MetricsReporter> reporters = CommonClientConfigs.metricsReporters("clientId", config);
        assertEquals(1, reporters.size());
        assertTrue(reporters.get(0) instanceof JmxReporter);

        config = new TestConfig(Collections.singletonMap(CommonClientConfigs.AUTO_INCLUDE_JMX_REPORTER_CONFIG, "false"));
        reporters = CommonClientConfigs.metricsReporters("clientId", config);
        assertTrue(reporters.isEmpty());

        config = new TestConfig(Collections.singletonMap(CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG, JmxReporter.class.getName()));
        reporters = CommonClientConfigs.metricsReporters("clientId", config);
        assertEquals(1, reporters.size());
        assertTrue(reporters.get(0) instanceof JmxReporter);

        Map<String, String> props = new HashMap<>();
        props.put(CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG, JmxReporter.class.getName() + "," + MyJmxReporter.class.getName());
        config = new TestConfig(props);
        reporters = CommonClientConfigs.metricsReporters("clientId", config);
        assertEquals(2, reporters.size());
    }

    public static class MyJmxReporter extends JmxReporter {
        public MyJmxReporter() {}
    }
}
