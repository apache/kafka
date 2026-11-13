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

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ShareSinkConnectorConfigTest {

    @Test
    void ordinaryConsumerRemainsTheDefaultWithExistingErrorHandling() {
        ConfigDef.ConfigKey mode = SinkConnectorConfig.configDef().configKeys().get("consumer.type");
        assertNotNull(mode, "Sink consumer mode must be part of the public configuration");
        assertEquals("consumer", mode.defaultValue);
        Map<String, String> props = properties("consumer");
        props.remove("consumer.type");
        props.remove("topics");
        props.put("topics.regex", "events-.*");
        props.put("errors.tolerance", "all");
        props.put("errors.deadletterqueue.topic.name", "failed-records");
        assertValid(validate(props));
    }

    @ParameterizedTest
    @CsvSource({
        "consumer.override.share.acknowledgement.mode, implicit",
        "consumer.override.share.acquire.mode, batch_optimized"
    })
    void rejectsUnsafeShareDeliveryModes(String key, String value) {
        Map<String, String> props = properties("share");
        props.put(key, value);
        assertError(validate(props), key);
    }

    @Test
    void acceptsExplicitBoundedShareSettings() {
        Map<String, String> props = properties(" share ");
        props.put("share.group.id", "orders-share");
        props.put("share.processing.max.age.ms", "120000");
        props.put("share.processing.cancel.grace.ms", "10000");
        props.put("errors.tolerance", " none ");
        props.put("consumer.override.share.acknowledgement.mode", "explicit");
        props.put("consumer.override.share.acquire.mode", "record_limit");
        Map<String, ConfigValue> values = validate(props);
        ConfigValue mode = values.get("consumer.type");
        assertNotNull(mode, "Share mode must be recognised, not silently ignored");
        assertEquals("share", mode.value());
        assertValid(values);
    }

    @Test
    void requiresProcessingBoundsAndRejectsIndependentErrorCompletion() {
        Map<String, String> props = properties("share");
        props.remove("topics");
        props.put("topics.regex", "events-.*");
        props.put("errors.tolerance", "all");
        props.put("errors.deadletterqueue.topic.name", "failed-records");
        Map<String, ConfigValue> values = validate(props);
        for (String key : List.of("share.processing.max.age.ms", "share.processing.cancel.grace.ms",
                "topics.regex", "errors.tolerance", "errors.deadletterqueue.topic.name")) {
            assertError(values, key);
        }

        props.put("share.processing.max.age.ms", "0");
        props.put("share.processing.cancel.grace.ms", "-1");
        values = validate(props);
        assertError(values, "share.processing.max.age.ms");
        assertError(values, "share.processing.cancel.grace.ms");
    }

    @Test
    void preservesCanonicalIdentityAndRejectsOrdinaryOnlyOverrides() {
        Map<String, String> props = properties("share");
        props.put("share.processing.max.age.ms", "1");
        props.put("share.processing.cancel.grace.ms", "0");
        assertValid(validate(props));

        props.put("share.group.id", "orders-share");
        props.put("consumer.override.group.id", "another-history");
        props.put("consumer.override.enable.auto.commit", "false");
        Map<String, ConfigValue> values = validate(props);
        assertError(values, "consumer.override.group.id");
        assertError(values, "consumer.override.enable.auto.commit");
    }

    private Map<String, String> properties(String mode) {
        return new HashMap<>(Map.of(
                "name", "share-config-test",
                "connector.class", "org.example.SinkConnector",
                "topics", "events",
                "consumer.type", mode));
    }

    private Map<String, ConfigValue> validate(Map<String, String> props) {
        Map<String, ConfigValue> values = new HashMap<>(SinkConnectorConfig.configDef().validateAll(props));
        SinkConnectorConfig.validate(props, values);
        return values;
    }

    private void assertError(Map<String, ConfigValue> values, String key) {
        ConfigValue value = values.get(key);
        assertNotNull(value, "Missing validation result for " + key);
        assertFalse(value.errorMessages().isEmpty(), "Expected a validation error for " + key);
    }

    private void assertValid(Map<String, ConfigValue> values) {
        values.forEach((key, value) -> assertTrue(value.errorMessages().isEmpty(),
                () -> key + ": " + value.errorMessages()));
    }
}
