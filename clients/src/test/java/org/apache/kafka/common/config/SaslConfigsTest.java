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
package org.apache.kafka.common.config;

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class SaslConfigsTest {
    @Test
    public void testSaslLoginRefreshDefaults() {
        Map<String, Object> vals = new ConfigDef().withClientSaslSupport().parse(Collections.emptyMap());
        assertEquals(SaslConfigs.DEFAULT_LOGIN_REFRESH_WINDOW_FACTOR,
                vals.get(SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_FACTOR));
        assertEquals(SaslConfigs.DEFAULT_LOGIN_REFRESH_WINDOW_JITTER,
                vals.get(SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_JITTER));
        assertEquals(SaslConfigs.DEFAULT_LOGIN_REFRESH_MIN_PERIOD_SECONDS,
                vals.get(SaslConfigs.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS));
        assertEquals(SaslConfigs.DEFAULT_LOGIN_REFRESH_BUFFER_SECONDS,
                vals.get(SaslConfigs.SASL_LOGIN_REFRESH_BUFFER_SECONDS));
    }

    @Test
    public void testSaslLoginRefreshMinValuesAreValid() {
        Map<Object, Object> props = new HashMap<>();
        props.put(SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_FACTOR, "0.5");
        props.put(SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_JITTER, "0.0");
        props.put(SaslConfigs.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS, "0");
        props.put(SaslConfigs.SASL_LOGIN_REFRESH_BUFFER_SECONDS, "0");
        Map<String, Object> vals = new ConfigDef().withClientSaslSupport().parse(props);
        assertEquals(Double.valueOf("0.5"), vals.get(SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_FACTOR));
        assertEquals(Double.valueOf("0.0"), vals.get(SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_JITTER));
        assertEquals(Short.valueOf("0"), vals.get(SaslConfigs.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS));
        assertEquals(Short.valueOf("0"), vals.get(SaslConfigs.SASL_LOGIN_REFRESH_BUFFER_SECONDS));
    }

    @Test
    public void testSaslLoginRefreshMaxValuesAreValid() {
        Map<Object, Object> props = new HashMap<>();
        props.put(SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_FACTOR, "1.0");
        props.put(SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_JITTER, "0.25");
        props.put(SaslConfigs.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS, "900");
        props.put(SaslConfigs.SASL_LOGIN_REFRESH_BUFFER_SECONDS, "3600");
        Map<String, Object> vals = new ConfigDef().withClientSaslSupport().parse(props);
        assertEquals(Double.valueOf("1.0"), vals.get(SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_FACTOR));
        assertEquals(Double.valueOf("0.25"), vals.get(SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_JITTER));
        assertEquals(Short.valueOf("900"), vals.get(SaslConfigs.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS));
        assertEquals(Short.valueOf("3600"), vals.get(SaslConfigs.SASL_LOGIN_REFRESH_BUFFER_SECONDS));
    }

    @Test(expected = ConfigException.class)
    public void testSaslLoginRefreshWindowFactorMinValueIsReallyMinimum() {
        Map<Object, Object> props = new HashMap<>();
        props.put(SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_FACTOR, "0.499999");
        new ConfigDef().withClientSaslSupport().parse(props);
    }

    @Test(expected = ConfigException.class)
    public void testSaslLoginRefreshWindowFactorMaxValueIsReallyMaximum() {
        Map<Object, Object> props = new HashMap<>();
        props.put(SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_FACTOR, "1.0001");
        new ConfigDef().withClientSaslSupport().parse(props);
    }

    @Test(expected = ConfigException.class)
    public void testSaslLoginRefreshWindowJitterMinValueIsReallyMinimum() {
        Map<Object, Object> props = new HashMap<>();
        props.put(SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_JITTER, "-0.000001");
        new ConfigDef().withClientSaslSupport().parse(props);
    }

    @Test(expected = ConfigException.class)
    public void testSaslLoginRefreshWindowJitterMaxValueIsReallyMaximum() {
        Map<Object, Object> props = new HashMap<>();
        props.put(SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_JITTER, "0.251");
        new ConfigDef().withClientSaslSupport().parse(props);
    }

    @Test(expected = ConfigException.class)
    public void testSaslLoginRefreshMinPeriodSecondsMinValueIsReallyMinimum() {
        Map<Object, Object> props = new HashMap<>();
        props.put(SaslConfigs.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS, "-1");
        new ConfigDef().withClientSaslSupport().parse(props);
    }

    @Test(expected = ConfigException.class)
    public void testSaslLoginRefreshMinPeriodSecondsMaxValueIsReallyMaximum() {
        Map<Object, Object> props = new HashMap<>();
        props.put(SaslConfigs.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS, "901");
        new ConfigDef().withClientSaslSupport().parse(props);
    }

    @Test(expected = ConfigException.class)
    public void testSaslLoginRefreshBufferSecondsMinValueIsReallyMinimum() {
        Map<Object, Object> props = new HashMap<>();
        props.put(SaslConfigs.SASL_LOGIN_REFRESH_BUFFER_SECONDS, "-1");
        new ConfigDef().withClientSaslSupport().parse(props);
    }

    @Test(expected = ConfigException.class)
    public void testSaslLoginRefreshBufferSecondsMaxValueIsReallyMaximum() {
        Map<Object, Object> props = new HashMap<>();
        props.put(SaslConfigs.SASL_LOGIN_REFRESH_BUFFER_SECONDS, "3601");
        new ConfigDef().withClientSaslSupport().parse(props);
    }
}
