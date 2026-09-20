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

import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ClusterTestDefaults(types = { Type.KRAFT })
public class AdminClientTimeoutIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(AdminClientTimeoutIntegrationTest.class);

    private final ClusterInstance clusterInstance;

    public AdminClientTimeoutIntegrationTest(ClusterInstance clusterInstance) {
        this.clusterInstance = clusterInstance;
    }

    /**
     * Test injecting timeouts for calls that are in flight.
     */
    @ClusterTest
    public void testCallInFlightTimeouts() throws Exception {
        var config = Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers(),
                AdminClientConfig.RETRIES_CONFIG, "0",
                // Set an extremely large overall API timeout to ensure the
                // FailureInjectingTimeoutProcessor triggers before the API-level timeout does.
                AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "100000000"
        );

        var factory = new FailureInjectingTimeoutProcessorFactory();
        try (var client = KafkaAdminClient.createInternal(new AdminClientConfig(config), factory)) {

            var future = client.createTopics(Stream.of("mytopic1", "mytopic2")
                    .map(t -> new NewTopic(t, 1, (short) 1)).toList(),
                    new CreateTopicsOptions().validateOnly(true)).all();

            var e = assertThrows(ExecutionException.class, future::get);
            assertInstanceOf(TimeoutException.class, e.getCause());

            var future2 = client.createTopics(Stream.of("mytopic3", "mytopic4")
                    .map(t -> new NewTopic(t, 1, (short) 1)).toList(),
                    new CreateTopicsOptions().validateOnly(true)).all();
            future2.get();
            assertEquals(1, factory.failuresInjected());
        }
    }


    static class FailureInjectingTimeoutProcessorFactory extends KafkaAdminClient.TimeoutProcessorFactory {

        private int numTries = 0;

        private int failuresInjected = 0;

        @Override
        KafkaAdminClient.TimeoutProcessor create(long now) {
            return new FailureInjectingTimeoutProcessor(now);
        }

        synchronized boolean shouldInjectFailure() {
            numTries++;
            if (numTries == 1) {
                failuresInjected++;
                return true;
            }
            return false;
        }

        synchronized int failuresInjected() {
            return failuresInjected;
        }

        final class FailureInjectingTimeoutProcessor extends KafkaAdminClient.TimeoutProcessor {
            FailureInjectingTimeoutProcessor(long now) {
                super(now);
            }

            @Override
            boolean callHasExpired(KafkaAdminClient.Call call) {
                if ((!call.isInternal()) && shouldInjectFailure()) {
                    log.debug("Injecting timeout for {}.", call);
                    return true;
                } else {
                    boolean ret = super.callHasExpired(call);
                    log.debug("callHasExpired({}) = {}", call, ret);
                    return ret;
                }
            }
        }
    }
}
