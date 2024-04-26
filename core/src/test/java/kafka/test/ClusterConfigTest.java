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
import java.util.Collections;
import java.util.Optional;

public class ClusterConfigTest {

    @Test
    public void testClusterConfigBuilder() throws IOException {
        File trustStoreFile = TestUtils.tempFile();

        ClusterConfig clusterConfig = ClusterConfig.builder()
                .setType(Type.KRAFT)
                .setBrokers(1)
                .setControllers(1)
                .setName("builder-test")
                .setAutoStart(true)
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
                .setPerBrokerProperties(Collections.singletonMap(0, Collections.singletonMap("broker_0", "broker_0_value")))
                .build();

        Assertions.assertEquals(Type.KRAFT, clusterConfig.clusterType());
        Assertions.assertEquals(1, clusterConfig.numBrokers());
        Assertions.assertEquals(1, clusterConfig.numControllers());
        Assertions.assertEquals(Optional.of("builder-test"), clusterConfig.name());
        Assertions.assertTrue(clusterConfig.isAutoStart());
        Assertions.assertEquals(SecurityProtocol.PLAINTEXT, clusterConfig.securityProtocol());
        Assertions.assertEquals(Optional.of("EXTERNAL"), clusterConfig.listenerName());
        Assertions.assertEquals(Optional.of(trustStoreFile), clusterConfig.trustStoreFile());
        Assertions.assertEquals(MetadataVersion.IBP_0_8_0, clusterConfig.metadataVersion());
        Assertions.assertEquals(Collections.singletonMap("broker", "broker_value"), clusterConfig.serverProperties());
        Assertions.assertEquals(Collections.singletonMap("consumer", "consumer_value"), clusterConfig.consumerProperties());
        Assertions.assertEquals(Collections.singletonMap("producer", "producer_value"), clusterConfig.producerProperties());
        Assertions.assertEquals(Collections.singletonMap("admin_client", "admin_client_value"), clusterConfig.adminClientProperties());
        Assertions.assertEquals(Collections.singletonMap("sasl_client", "sasl_client_value"), clusterConfig.saslClientProperties());
        Assertions.assertEquals(Collections.singletonMap("sasl_server", "sasl_server_value"), clusterConfig.saslServerProperties());
        Assertions.assertEquals(Collections.singletonMap(0, Collections.singletonMap("broker_0", "broker_0_value")), clusterConfig.perBrokerOverrideProperties());

        ClusterConfig copy = ClusterConfig.builder(clusterConfig).build();

        Assertions.assertEquals(clusterConfig.clusterType(), copy.clusterType());
        Assertions.assertEquals(clusterConfig.numBrokers(), copy.numBrokers());
        Assertions.assertEquals(clusterConfig.numControllers(), copy.numControllers());
        Assertions.assertEquals(clusterConfig.name(), copy.name());
        Assertions.assertEquals(clusterConfig.isAutoStart(), copy.isAutoStart());
        Assertions.assertEquals(clusterConfig.securityProtocol(), copy.securityProtocol());
        Assertions.assertEquals(clusterConfig.listenerName(), copy.listenerName());
        Assertions.assertEquals(clusterConfig.trustStoreFile(), copy.trustStoreFile());
        Assertions.assertEquals(clusterConfig.metadataVersion(), copy.metadataVersion());
        Assertions.assertEquals(clusterConfig.serverProperties(), copy.serverProperties());
        Assertions.assertEquals(clusterConfig.consumerProperties(), copy.consumerProperties());
        Assertions.assertEquals(clusterConfig.producerProperties(), copy.producerProperties());
        Assertions.assertEquals(clusterConfig.adminClientProperties(), copy.adminClientProperties());
        Assertions.assertEquals(clusterConfig.saslClientProperties(), copy.saslClientProperties());
        Assertions.assertEquals(clusterConfig.saslServerProperties(), copy.saslServerProperties());
        Assertions.assertEquals(clusterConfig.perBrokerOverrideProperties(), copy.perBrokerOverrideProperties());
    }
}
