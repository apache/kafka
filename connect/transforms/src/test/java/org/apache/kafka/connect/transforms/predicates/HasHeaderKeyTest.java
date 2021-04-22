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
package org.apache.kafka.connect.transforms.predicates;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class HasHeaderKeyTest {

    @Test
    public void testNameRequiredInConfig() {
        Map<String, String> props = new HashMap<>();
        ConfigException e = assertThrows(ConfigException.class, () -> config(props));
        assertTrue(e.getMessage().contains("Missing required configuration \"name\""));
    }

    @Test
    public void testNameMayNotBeEmptyInConfig() {
        Map<String, String> props = new HashMap<>();
        props.put("name", "");
        ConfigException e = assertThrows(ConfigException.class, () -> config(props));
        assertTrue(e.getMessage().contains("String must be non-empty"));
    }

    @Test
    public void testConfig() {
        HasHeaderKey<SourceRecord> predicate = new HasHeaderKey<>();
        predicate.config().validate(Collections.singletonMap("name", "foo"));

        List<ConfigValue> configs = predicate.config().validate(Collections.singletonMap("name", ""));
        assertEquals(singletonList("Invalid value  for configuration name: String must be non-empty"), configs.get(0).errorMessages());
    }

    @Test
    public void testTest() {
        HasHeaderKey<SourceRecord> predicate = new HasHeaderKey<>();
        predicate.configure(Collections.singletonMap("name", "foo"));

        assertTrue(predicate.test(recordWithHeaders("foo")));
        assertTrue(predicate.test(recordWithHeaders("foo", "bar")));
        assertTrue(predicate.test(recordWithHeaders("bar", "foo", "bar", "foo")));
        assertFalse(predicate.test(recordWithHeaders("bar")));
        assertFalse(predicate.test(recordWithHeaders("bar", "bar")));
        assertFalse(predicate.test(recordWithHeaders()));
        assertFalse(predicate.test(new SourceRecord(null, null, null, null, null)));

    }

    private SimpleConfig config(Map<String, String> props) {
        return new SimpleConfig(new HasHeaderKey().config(), props);
    }

    private SourceRecord recordWithHeaders(String... headers) {
        return new SourceRecord(null, null, null, null, null, null, null, null, null,
                Arrays.stream(headers).map(TestHeader::new).collect(Collectors.toList()));
    }

    private static class TestHeader implements Header {

        private final String key;

        public TestHeader(String key) {
            this.key = key;
        }

        @Override
        public String key() {
            return key;
        }

        @Override
        public Schema schema() {
            return null;
        }

        @Override
        public Object value() {
            return null;
        }

        @Override
        public Header with(Schema schema, Object value) {
            return null;
        }

        @Override
        public Header rename(String key) {
            return null;
        }
    }
}
