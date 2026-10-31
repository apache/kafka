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
package org.apache.kafka.clients.admin;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.DefaultHostResolver;
import org.apache.kafka.clients.HostResolver;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.common.utils.Utils;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Prototype/spike: proves that KafkaAdminClient#createInternal's HostResolver injection point
 * (the same one ConcurrentListOffsetsRequestTest uses) now also controls KIP-909 async bootstrap
 * DNS resolution, after routing NetworkClient's bootstrap resolution through the injected
 * HostResolver instead of calling InetAddress/InetSocketAddress directly.
 */
@ClusterTestDefaults(
        types = {Type.KRAFT},
        brokers = 1
)
public class AsyncBootstrapResolutionTest {
    private final ClusterInstance clusterInstance;

    AsyncBootstrapResolutionTest(ClusterInstance clusterInstance) {
        this.clusterInstance = clusterInstance;
    }

    @ClusterTest
    public void adminClientRecoversAfterTransientBootstrapDnsFailure() throws Exception {
        clusterInstance.waitForReadyBrokers();

        String bootstrapServers = clusterInstance.bootstrapServers();
        String realHost = Utils.getHost(bootstrapServers);
        Integer realPort = Utils.getPort(bootstrapServers);
        InetAddress realAddress = new DefaultHostResolver().resolve(realHost)[0];

        // Use a hostname that real JVM DNS cannot resolve (RFC 6761 reserved .invalid TLD) as the
        // configured bootstrap host, paired with the real broker's port. This way, if bootstrap
        // resolution ever bypasses the injected resolver again, this test fails for real instead
        // of passing incidentally because the real cluster hostname happens to resolve on its own.
        String unresolvableBootstrapHost = "async-bootstrap-resolution-test.invalid";

        AtomicInteger attempts = new AtomicInteger();
        HostResolver flakyThenWorking = resolveHost -> {
            if (attempts.getAndIncrement() < 2) {
                throw new UnknownHostException("Simulated transient DNS failure for " + resolveHost);
            }
            return new InetAddress[] {realAddress};
        };

        Map<String, Object> props = Map.of(
                CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, unresolvableBootstrapHost + ":" + realPort,
                CommonClientConfigs.BOOTSTRAP_RESOLVE_TIMEOUT_MS_CONFIG, "30000");

        try (Admin admin = KafkaAdminClient.createInternal(
                new AdminClientConfig(clusterInstance.setClientSaslConfig(props), true),
                null,
                flakyThenWorking)) {

            assertDoesNotThrow(() -> admin.listTopics().names().get(20, TimeUnit.SECONDS));
            assertTrue(attempts.get() >= 3,
                    "Expected at least 2 failed resolution attempts before success, got " + attempts.get());
        }
    }
}
