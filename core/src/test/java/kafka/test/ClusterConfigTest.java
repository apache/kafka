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

package kafka.test;

import kafka.test.annotation.Type;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

public class ClusterConfigTest {

    private static Map<String, Object> fields(ClusterConfig config) {
        return Arrays.stream(config.getClass().getDeclaredFields()).collect(Collectors.toMap(Field::getName, f -> {
            f.setAccessible(true);
            return Assertions.assertDoesNotThrow(() -> f.get(config));
        }));
    }

    @Test
    public void testCopy() throws IOException {
        File trustStoreFile = TestUtils.tempFile();

        ClusterConfig clusterConfig = ClusterConfig.builder()
                .setTypes(Collections.singleton(Type.KRAFT))
                .setBrokers(3)
                .setControllers(2)
                .setDisksPerBroker(1)
                .setAutoStart(true)
                .setTags(Arrays.asList("name", "Generated Test"))
                .setSecurityProtocol(SecurityProtocol.PLAINTEXT)
                .setListenerName("EXTERNAL")
                .setTrustStoreFile(trustStoreFile)
                .setMetadataVersion(MetadataVersion.IBP_0_8_0)
                .setServerProperties(Collections.singletonMap("broker", "broker_value"))
                .setConsumerProperties(Collections.singletonMap("consumer", "consumer_value"))
                .setProducerProperties(Collections.singletonMap("producer", "producer_value"))
                .setAdminClientProperties(Collections.singletonMap("admin_client", "admin_client_value"))
                .setSaslClientProperties(Collections.singletonMap("sasl_client", "sasl_client_value"))
                .setSaslServerProperties(Collections.singletonMap("sasl_server", "sasl_server_value"))
                .setPerServerProperties(Collections.singletonMap(0, Collections.singletonMap("broker_0", "broker_0_value")))
                .build();

        Map<String, Object> clusterConfigFields = fields(clusterConfig);
        Map<String, Object> copyFields = fields(clusterConfig);
        Assertions.assertEquals(clusterConfigFields, copyFields);
    }

    @Test
    public void testBrokerLessThanZero() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> ClusterConfig.builder()
                .setBrokers(-1)
                .setControllers(1)
                .setDisksPerBroker(1)
                .build());
    }

    @Test
    public void testControllersLessThanZero() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> ClusterConfig.builder()
                .setBrokers(1)
                .setControllers(-1)
                .setDisksPerBroker(1)
                .build());
    }

    @Test
    public void testDisksPerBrokerIsZero() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> ClusterConfig.builder()
                .setBrokers(1)
                .setControllers(1)
                .setDisksPerBroker(0)
                .build());
    }
}
