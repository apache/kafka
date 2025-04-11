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

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.connect.runtime.WorkerConfig;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class StandaloneConfigTest {

    private static final String HTTPS_LISTENER_PREFIX = "listeners.https.";

    private Map<String, Object> sslProps() {
        return new HashMap<>() {
            {
                put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, new Password("ssl_key_password"));
                put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "ssl_keystore");
                put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, new Password("ssl_keystore_password"));
                put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "ssl_truststore");
                put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, new Password("ssl_truststore_password"));
            }
        };
    }

    private Map<String, String> baseWorkerProps() {
        return new HashMap<>() {
            {
                put(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
                put(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
                put(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, "/tmp/foo");
            }
        };
    }

    private static Map<String, String> withStringValues(Map<String, ?> inputs, String prefix) {
        return ConfigDef.convertToStringMapWithPasswordValues(inputs).entrySet().stream()
            .collect(Collectors.toMap(
                entry -> prefix + entry.getKey(),
                Map.Entry::getValue
            ));
    }

    @Test
    public void testRestServerPrefixedSslConfigs() {
        Map<String, String> workerProps = baseWorkerProps();
        Map<String, Object> expectedSslProps = sslProps();
        workerProps.putAll(withStringValues(expectedSslProps, HTTPS_LISTENER_PREFIX));

        StandaloneConfig config = new StandaloneConfig(workerProps);
        assertEquals(expectedSslProps, config.valuesWithPrefixAllOrNothing(HTTPS_LISTENER_PREFIX));
    }

    @Test
    public void testRestServerNonPrefixedSslConfigs() {
        Map<String, String> props = baseWorkerProps();
        Map<String, Object> expectedSslProps = sslProps();
        props.putAll(withStringValues(expectedSslProps, ""));

        StandaloneConfig config = new StandaloneConfig(props);
        Map<String, Object> actualProps = config.valuesWithPrefixAllOrNothing(HTTPS_LISTENER_PREFIX)
            .entrySet().stream()
            .filter(entry -> expectedSslProps.containsKey(entry.getKey()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        assertEquals(expectedSslProps, actualProps);
    }
}
