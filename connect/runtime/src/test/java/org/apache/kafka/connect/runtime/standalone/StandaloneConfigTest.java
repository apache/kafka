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
package org.apache.kafka.connect.runtime.standalone;

import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

public class StandaloneConfigTest {

    private static final String HTTPS_LISTENER_PREFIX = "listeners.https.";

    @Test
    public void testRestServerPrefixedSslConfigs() {
        Map<String, String> httpsListenerProps = new HashMap<String, String>() {
            {
                put(HTTPS_LISTENER_PREFIX + SslConfigs.SSL_KEY_PASSWORD_CONFIG, "ssl_key_password");
                put(HTTPS_LISTENER_PREFIX + SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "ssl_keystore");
                put(HTTPS_LISTENER_PREFIX + SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "ssk_keystore_password");
                put(HTTPS_LISTENER_PREFIX + SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "ssl_truststore");
                put(HTTPS_LISTENER_PREFIX + SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "ssl_truststore_password");
            }
        };

        Set<String> passwordConfigs = Stream.of(
            SslConfigs.SSL_KEY_PASSWORD_CONFIG,
            SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG,
            SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG
        ).map(key -> HTTPS_LISTENER_PREFIX + key)
            .collect(Collectors.toSet());

        Map<String, Object> expectedListenerProps = httpsListenerProps.entrySet().stream()
            .collect(Collectors.toMap(
                entry -> entry.getKey().substring(HTTPS_LISTENER_PREFIX.length()),
                entry -> passwordConfigs.contains(entry.getKey())
                        ? new Password(entry.getValue())
                        : entry.getValue()
            ));

        Map<String, String> props = new HashMap<String, String>() {
            {
                // Base props required for standalone mode
                put(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
                put(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
                put(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, "/tmp/foad");

                // Custom props for test
                putAll(httpsListenerProps);
            }
        };
    
        StandaloneConfig config = new StandaloneConfig(props);
        Map<String, Object> actualHttpsListenerProps = config.valuesWithPrefixAllOrNothing(HTTPS_LISTENER_PREFIX);
        assertEquals(expectedListenerProps, actualHttpsListenerProps);
    }

    @Test
    public void testRestServerNonPrefixedSslConfigs() {
        Map<String, String> httpsListenerProps = new HashMap<String, String>() {
            {
                put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, "ssl_key_password");
                put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "ssl_keystore");
                put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "ssk_keystore_password");
                put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "ssl_truststore");
                put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "ssl_truststore_password");
            }
        };

        Set<String> passwordConfigs = Stream.of(
            SslConfigs.SSL_KEY_PASSWORD_CONFIG,
            SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG,
            SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG
        ).collect(Collectors.toSet());

        Map<String, Object> expectedListenerProps = httpsListenerProps.entrySet().stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> passwordConfigs.contains(entry.getKey())
                    ? new Password(entry.getValue())
                    : entry.getValue()
            ));

        Map<String, String> props = new HashMap<String, String>() {
            {
                // Base props required for standalone mode
                put(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
                put(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
                put(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, "/tmp/foad");

                // Custom props for test
                putAll(httpsListenerProps);
            }
        };

        StandaloneConfig config = new StandaloneConfig(props);
        Map<String, Object> actualHttpsListenerProps = config.valuesWithPrefixAllOrNothing(HTTPS_LISTENER_PREFIX)
            .entrySet().stream()
            .filter(entry -> expectedListenerProps.containsKey(entry.getKey()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        assertEquals(expectedListenerProps, actualHttpsListenerProps);
    }
}
