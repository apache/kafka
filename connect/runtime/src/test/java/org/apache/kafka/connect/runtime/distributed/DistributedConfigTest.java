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

package org.apache.kafka.connect.runtime.distributed;

import org.apache.kafka.common.config.ConfigException;
import org.junit.Test;

import javax.crypto.KeyGenerator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

public class DistributedConfigTest {

    public Map<String, String> configs() {
        Map<String, String> result = new HashMap<>();
        result.put(DistributedConfig.GROUP_ID_CONFIG, "connect-cluster");
        result.put(DistributedConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        result.put(DistributedConfig.CONFIG_TOPIC_CONFIG, "connect-configs");
        result.put(DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG, "connect-offsets");
        result.put(DistributedConfig.STATUS_STORAGE_TOPIC_CONFIG, "connect-status");
        result.put(DistributedConfig.KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        result.put(DistributedConfig.VALUE_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        return result;
    }

    @Test
    public void shouldCreateKeyGeneratorWithDefaultSettings() {
        DistributedConfig config = new DistributedConfig(configs());
        assertNotNull(config.getInternalRequestKeyGenerator());
    }

    @Test
    public void shouldCreateKeyGeneratorWithSpecificSettings() {
        final String algorithm = "HmacSHA1";
        Map<String, String> configs = configs();
        configs.put(DistributedConfig.INTER_WORKER_KEY_GENERATION_ALGORITHM_CONFIG, algorithm);
        configs.put(DistributedConfig.INTER_WORKER_KEY_SIZE_CONFIG, "512");
        configs.put(DistributedConfig.INTER_WORKER_VERIFICATION_ALGORITHMS_CONFIG, algorithm);
        DistributedConfig config = new DistributedConfig(configs);
        KeyGenerator keyGenerator = config.getInternalRequestKeyGenerator();
        assertNotNull(keyGenerator);
        assertEquals(algorithm, keyGenerator.getAlgorithm());
        assertEquals(512 / 8, keyGenerator.generateKey().getEncoded().length);
    }

    @Test(expected = ConfigException.class)
    public void shouldFailWithEmptyListOfVerificationAlgorithms() {
        Map<String, String> configs = configs();
        configs.put(DistributedConfig.INTER_WORKER_VERIFICATION_ALGORITHMS_CONFIG, "");
        new DistributedConfig(configs);
    }

    @Test(expected = ConfigException.class)
    public void shouldFailIfKeyAlgorithmNotInVerificationAlgorithmsList() {
        Map<String, String> configs = configs();
        configs.put(DistributedConfig.INTER_WORKER_KEY_GENERATION_ALGORITHM_CONFIG, "HmacSHA1");
        configs.put(DistributedConfig.INTER_WORKER_VERIFICATION_ALGORITHMS_CONFIG, "HmacSHA256");
        new DistributedConfig(configs);
    }

    @Test(expected = ConfigException.class)
    public void shouldFailWithInvalidKeyAlgorithm() {
        Map<String, String> configs = configs();
        configs.put(DistributedConfig.INTER_WORKER_KEY_GENERATION_ALGORITHM_CONFIG, "not-actually-a-key-algorithm");
        new DistributedConfig(configs);
    }

    @Test(expected = ConfigException.class)
    public void shouldFailWithInvalidKeySize() {
        Map<String, String> configs = configs();
        configs.put(DistributedConfig.INTER_WORKER_KEY_SIZE_CONFIG, "0");
        new DistributedConfig(configs);
    }

    @Test
    public void shouldValidateAllVerificationAlgorithms() {
        List<String> algorithms =
            new ArrayList<>(Arrays.asList("HmacSHA1", "HmacSHA256", "HmacMD5", "bad-algorithm"));
        Map<String, String> configs = configs();
        for (int i = 0; i < algorithms.size(); i++) {
            configs.put(DistributedConfig.INTER_WORKER_VERIFICATION_ALGORITHMS_CONFIG, String.join(",", algorithms));
            assertThrows(ConfigException.class, () -> new DistributedConfig(configs));
            algorithms.add(algorithms.remove(0));
        }
    }
}
