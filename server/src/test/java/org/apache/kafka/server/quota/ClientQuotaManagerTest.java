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
package org.apache.kafka.server.quota;

import org.apache.kafka.common.internals.Plugin;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Quota;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.network.Session;
import org.apache.kafka.server.config.ClientQuotaManagerConfig;

import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ClientQuotaManagerTest extends BaseClientQuotaManagerTest {

    private final ClientQuotaManagerConfig config = new ClientQuotaManagerConfig();

    private void withQuotaManager(QuotaType quotaType, ClientQuotaManagerConfig config, Consumer<ClientQuotaManager> f) {
        ClientQuotaManager quotaManager = new ClientQuotaManager(config, metrics, quotaType, time, "");
        try {
            f.accept(quotaManager);
        } finally {
            quotaManager.shutdown();
        }
    }

    private void withQuotaManager(QuotaType quotaType, Consumer<ClientQuotaManager> f) {
        withQuotaManager(quotaType, config, f);
    }

    private void testQuotaParsing(ClientQuotaManagerConfig config, UserClient client1, UserClient client2, UserClient randomClient, UserClient defaultConfigClient) {
        withQuotaManager(QuotaType.PRODUCE, clientQuotaManager -> {
            // Case 1: Update the quota. Assert that the new quota value is returned
            clientQuotaManager.updateQuota(
                    client1.configUser,
                    client1.configClientEntity,
                    Optional.of(new Quota(2000, true))
            );
            clientQuotaManager.updateQuota(
                    client2.configUser,
                    client2.configClientEntity,
                    Optional.of(new Quota(4000, true))
            );

            assertEquals(Long.MAX_VALUE, clientQuotaManager.quota(randomClient.user, randomClient.clientId).bound(), 0.0,
                    "Default producer quota should be " + Long.MAX_VALUE);
            assertEquals(2000, clientQuotaManager.quota(client1.user, client1.clientId).bound(), 0.0,
                    "Should return the overridden value (2000)");
            assertEquals(4000, clientQuotaManager.quota(client2.user, client2.clientId).bound(), 0.0,
                    "Should return the overridden value (4000)");

            // p1 should be throttled using the overridden quota
            int throttleTimeMs = maybeRecord(clientQuotaManager, client1.user, client1.clientId, 2500 * config.numQuotaSamples());
            assertTrue(throttleTimeMs > 0, "throttleTimeMs should be > 0. was " + throttleTimeMs);

            // Case 2: Change quota again. The quota should be updated within KafkaMetrics as well since the sensor was created.
            // p1 should no longer be throttled after the quota change
            clientQuotaManager.updateQuota(
                    client1.configUser,
                    client1.configClientEntity,
                    Optional.of(new Quota(3000, true))
            );
            assertEquals(3000, clientQuotaManager.quota(client1.user, client1.clientId).bound(), 0.0, "Should return the newly overridden value (3000)");

            throttleTimeMs = maybeRecord(clientQuotaManager, client1.user, client1.clientId, 0);
            assertEquals(0, throttleTimeMs, "throttleTimeMs should be 0. was " + throttleTimeMs);

            // Case 3: Change quota back to default. Should be throttled again
            clientQuotaManager.updateQuota(
                    client1.configUser,
                    client1.configClientEntity,
                    Optional.of(new Quota(500, true))
            );
            assertEquals(500, clientQuotaManager.quota(client1.user, client1.clientId).bound(), 0.0, "Should return the default value (500)");

            throttleTimeMs = maybeRecord(clientQuotaManager, client1.user, client1.clientId, 0);
            assertTrue(throttleTimeMs > 0, "throttleTimeMs should be > 0. was " + throttleTimeMs);

            // Case 4: Set high default quota, remove p1 quota. p1 should no longer be throttled
            clientQuotaManager.updateQuota(
                    client1.configUser,
                    client1.configClientEntity,
                    Optional.empty()
            );
            clientQuotaManager.updateQuota(
                    defaultConfigClient.configUser,
                    defaultConfigClient.configClientEntity,
                    Optional.of(new Quota(4000, true))
            );
            assertEquals(4000, clientQuotaManager.quota(client1.user, client1.clientId).bound(), 0.0, "Should return the newly overridden value (4000)");

            throttleTimeMs = maybeRecord(clientQuotaManager, client1.user, client1.clientId, 1000 * config.numQuotaSamples());
            assertEquals(0, throttleTimeMs, "throttleTimeMs should be 0. was " + throttleTimeMs);

        });
    }

    /**
     * Tests parsing for <user> quotas when client-id default quota properties are set.
     */
    @Test
    public void testUserQuotaParsingWithDefaultClientIdQuota() {
        UserClient client1 = new UserClient("User1", "p1", Optional.of(new ClientQuotaManager.UserEntity("User1")), Optional.empty());
        UserClient client2 = new UserClient("User2", "p2", Optional.of(new ClientQuotaManager.UserEntity("User2")), Optional.empty());
        UserClient randomClient = new UserClient("RandomUser", "random-client-id", Optional.empty(), Optional.empty());
        UserClient defaultConfigClient = new UserClient("", "", Optional.of(ClientQuotaManager.DEFAULT_USER_ENTITY), Optional.empty());
        testQuotaParsing(config, client1, client2, randomClient, defaultConfigClient);
    }

    private void checkQuota(ClientQuotaManager quotaManager, String user, String clientId, long expectedBound, int value, boolean expectThrottle) {
        assertEquals(expectedBound, quotaManager.quota(user, clientId).bound(), 0.0);
        Session session = new Session(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, user), assertDoesNotThrow(InetAddress::getLocalHost));
        double expectedMaxValueInQuotaWindow = expectedBound < Long.MAX_VALUE
            ? config.quotaWindowSizeSeconds() * (config.numQuotaSamples() - 1) * expectedBound
            : Double.MAX_VALUE;
        assertEquals(expectedMaxValueInQuotaWindow, quotaManager.maxValueInQuotaWindow(session, clientId), 0.01);

        int throttleTimeMs = maybeRecord(quotaManager, user, clientId, value * config.numQuotaSamples());
        if (expectThrottle) {
            assertTrue(throttleTimeMs > 0, "throttleTimeMs should be > 0. was " + throttleTimeMs);
        } else {
            assertEquals(0, throttleTimeMs, "throttleTimeMs should be 0. was " + throttleTimeMs);
        }
    }

    @Test
    public void testMaxValueInQuotaWindowWithNonDefaultQuotaWindow() {
        int numFullQuotaWindows = 3;   // 3 seconds window (vs. 10 seconds default)
        ClientQuotaManagerConfig nonDefaultConfig = new ClientQuotaManagerConfig(numFullQuotaWindows + 1);
        withQuotaManager(QuotaType.FETCH, nonDefaultConfig, clientQuotaManager -> {
            Session userSession = new Session(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "userA"), assertDoesNotThrow(InetAddress::getLocalHost));

            // no quota set
            assertEquals(Double.MAX_VALUE, clientQuotaManager.maxValueInQuotaWindow(userSession, "client1"), 0.01);

            // Set default <user> quota config
            clientQuotaManager.updateQuota(
                    Optional.of(ClientQuotaManager.DEFAULT_USER_ENTITY),
                    Optional.empty(),
                    Optional.of(new Quota(10, true))
            );
            assertEquals(10 * numFullQuotaWindows, clientQuotaManager.maxValueInQuotaWindow(userSession, "client1"), 0.01);
        });
    }

    @Test
    public void testSetAndRemoveDefaultUserQuota() {
        // quotaTypesEnabled will be QuotaTypes.NoQuotas initially
        withQuotaManager(QuotaType.PRODUCE, clientQuotaManager -> {
            // no quota set yet, should not throttle
            checkQuota(clientQuotaManager, "userA", "client1", Long.MAX_VALUE, 1000, false);

            // Set default <user> quota config
            clientQuotaManager.updateQuota(
                    Optional.of(ClientQuotaManager.DEFAULT_USER_ENTITY),
                    Optional.empty(),
                    Optional.of(new Quota(10, true))
            );
            checkQuota(clientQuotaManager, "userA", "client1", 10, 1000, true);

            // Remove default <user> quota config, back to no quotas
            clientQuotaManager.updateQuota(
                    Optional.of(ClientQuotaManager.DEFAULT_USER_ENTITY),
                    Optional.empty(),
                    Optional.empty()
            );
            checkQuota(clientQuotaManager, "userA", "client1", Long.MAX_VALUE, 1000, false);
        });
    }

    @Test
    public void testSetAndRemoveUserQuota() {
        withQuotaManager(QuotaType.PRODUCE, clientQuotaManager -> {
            // Set <user> quota config
            clientQuotaManager.updateQuota(
                    Optional.of(new ClientQuotaManager.UserEntity("userA")),
                    Optional.empty(),
                    Optional.of(new Quota(10, true))
            );
            checkQuota(clientQuotaManager, "userA", "client1", 10, 1000, true);

            // Remove <user> quota config, back to no quotas
            clientQuotaManager.updateQuota(
                    Optional.of(new ClientQuotaManager.UserEntity("userA")),
                    Optional.empty(),
                    Optional.empty()
            );
            checkQuota(clientQuotaManager, "userA", "client1", Long.MAX_VALUE, 1000, false);
        });
    }

    @Test
    public void testSetAndRemoveUserClientQuota() {
        // quotaTypesEnabled will be QuotaTypes.NoQuotas initially
        withQuotaManager(QuotaType.PRODUCE, clientQuotaManager -> {
            // Set <user, client-id> quota config
            clientQuotaManager.updateQuota(
                    Optional.of(new ClientQuotaManager.UserEntity("userA")),
                    Optional.of(new ClientQuotaManager.ClientIdEntity("client1")),
                    Optional.of(new Quota(10, true))
            );
            checkQuota(clientQuotaManager, "userA", "client1", 10, 1000, true);

            // Remove <user, client-id> quota config, back to no quotas
            clientQuotaManager.updateQuota(
                    Optional.of(new ClientQuotaManager.UserEntity("userA")),
                    Optional.of(new ClientQuotaManager.ClientIdEntity("client1")),
                    Optional.empty()
            );
            checkQuota(clientQuotaManager, "userA", "client1", Long.MAX_VALUE, 1000, false);
        });
    }

    @Test
    public void testQuotaConfigPrecedence() {
        withQuotaManager(QuotaType.PRODUCE, clientQuotaManager -> {
            clientQuotaManager.updateQuota(
                    Optional.of(ClientQuotaManager.DEFAULT_USER_ENTITY),
                    Optional.empty(),
                    Optional.of(new Quota(1000, true))
            );
            clientQuotaManager.updateQuota(
                    Optional.empty(),
                    Optional.of(ClientQuotaManager.DEFAULT_USER_CLIENT_ID),
                    Optional.of(new Quota(2000, true))
            );
            clientQuotaManager.updateQuota(
                    Optional.of(ClientQuotaManager.DEFAULT_USER_ENTITY),
                    Optional.of(ClientQuotaManager.DEFAULT_USER_CLIENT_ID),
                    Optional.of(new Quota(3000, true))
            );
            clientQuotaManager.updateQuota(
                    Optional.of(new ClientQuotaManager.UserEntity("userA")),
                    Optional.empty(),
                    Optional.of(new Quota(4000, true))
            );
            clientQuotaManager.updateQuota(
                    Optional.of(new ClientQuotaManager.UserEntity("userA")),
                    Optional.of(new ClientQuotaManager.ClientIdEntity("client1")),
                    Optional.of(new Quota(5000, true))
            );
            clientQuotaManager.updateQuota(
                    Optional.of(new ClientQuotaManager.UserEntity("userB")),
                    Optional.empty(),
                    Optional.of(new Quota(6000, true))
            );
            clientQuotaManager.updateQuota(
                    Optional.of(new ClientQuotaManager.UserEntity("userB")),
                    Optional.of(new ClientQuotaManager.ClientIdEntity("client1")),
                    Optional.of(new Quota(7000, true))
            );
            clientQuotaManager.updateQuota(
                    Optional.of(new ClientQuotaManager.UserEntity("userB")),
                    Optional.of(ClientQuotaManager.DEFAULT_USER_CLIENT_ID),
                    Optional.of(new Quota(8000, true))
            );
            clientQuotaManager.updateQuota(
                    Optional.of(new ClientQuotaManager.UserEntity("userC")),
                    Optional.empty(),
                    Optional.of(new Quota(10000, true))
            );
            clientQuotaManager.updateQuota(
                    Optional.empty(),
                    Optional.of(new ClientQuotaManager.ClientIdEntity("client1")),
                    Optional.of(new Quota(9000, true))
            );

            checkQuota(clientQuotaManager, "userA", "client1", 5000, 4500, false); // <user, client> quota takes precedence over <user>
            checkQuota(clientQuotaManager, "userA", "client2", 4000, 4500, true);  // <user> quota takes precedence over <client> and defaults
            checkQuota(clientQuotaManager, "userA", "client3", 4000, 0, true);     // <user> quota is shared across clients of user
            checkQuota(clientQuotaManager, "userA", "client1", 5000, 0, false);    // <user, client> is exclusive use, unaffected by other clients

            checkQuota(clientQuotaManager, "userB", "client1", 7000, 8000, true);
            checkQuota(clientQuotaManager, "userB", "client2", 8000, 7000, false); // Default per-client quota for exclusive use of <user, client>
            checkQuota(clientQuotaManager, "userB", "client3", 8000, 7000, false);

            checkQuota(clientQuotaManager, "userD", "client1", 3000, 3500, true);  // Default <user, client> quota
            checkQuota(clientQuotaManager, "userD", "client2", 3000, 2500, false);
            checkQuota(clientQuotaManager, "userE", "client1", 3000, 2500, false);

            // Remove default <user, client> quota config, revert to <user> default
            clientQuotaManager.updateQuota(
                    Optional.of(ClientQuotaManager.DEFAULT_USER_ENTITY),
                    Optional.of(ClientQuotaManager.DEFAULT_USER_CLIENT_ID),
                    Optional.empty()
            );
            checkQuota(clientQuotaManager, "userD", "client1", 1000, 0, false);    // Metrics tags changed, restart counter
            checkQuota(clientQuotaManager, "userE", "client4", 1000, 1500, true);
            checkQuota(clientQuotaManager, "userF", "client4", 1000, 800, false);  // Default <user> quota shared across clients of user
            checkQuota(clientQuotaManager, "userF", "client5", 1000, 800, true);

            // Remove default <user> quota config, revert to <client-id> default
            clientQuotaManager.updateQuota(
                    Optional.of(ClientQuotaManager.DEFAULT_USER_ENTITY),
                    Optional.empty(),
                    Optional.empty()
            );
            checkQuota(clientQuotaManager, "userF", "client4", 2000, 0, false); // Default <client-id> quota shared across client-id of all users
            checkQuota(clientQuotaManager, "userF", "client5", 2000, 0, false);
            checkQuota(clientQuotaManager, "userF", "client5", 2000, 2500, true);
            checkQuota(clientQuotaManager, "userG", "client5", 2000, 0, true);

            // Update quotas
            clientQuotaManager.updateQuota(
                    Optional.of(new ClientQuotaManager.UserEntity("userA")),
                    Optional.empty(),
                    Optional.of(new Quota(8000, true))
            );
            clientQuotaManager.updateQuota(
                    Optional.of(new ClientQuotaManager.UserEntity("userA")),
                    Optional.of(new ClientQuotaManager.ClientIdEntity("client1")),
                    Optional.of(new Quota(10000, true))
            );
            checkQuota(clientQuotaManager, "userA", "client2", 8000, 0, false);
            checkQuota(clientQuotaManager, "userA", "client2", 8000, 4500, true); // Throttled due to sum of new and earlier values
            checkQuota(clientQuotaManager, "userA", "client1", 10000, 0, false);
            checkQuota(clientQuotaManager, "userA", "client1", 10000, 6000, true);
            clientQuotaManager.updateQuota(
                    Optional.of(new ClientQuotaManager.UserEntity("userA")),
                    Optional.of(new ClientQuotaManager.ClientIdEntity("client1")),
                    Optional.empty()
            );
            checkQuota(clientQuotaManager, "userA", "client6", 8000, 0, true);   // Throttled due to shared user quota
            clientQuotaManager.updateQuota(
                    Optional.of(new ClientQuotaManager.UserEntity("userA")),
                    Optional.of(new ClientQuotaManager.ClientIdEntity("client6")),
                    Optional.of(new Quota(11000, true))
            );
            checkQuota(clientQuotaManager, "userA", "client6", 11000, 8500, false);
            clientQuotaManager.updateQuota(
                    Optional.of(new ClientQuotaManager.UserEntity("userA")),
                    Optional.of(ClientQuotaManager.DEFAULT_USER_CLIENT_ID),
                    Optional.of(new Quota(12000, true))
            );
            clientQuotaManager.updateQuota(
                    Optional.of(new ClientQuotaManager.UserEntity("userA")),
                    Optional.of(new ClientQuotaManager.ClientIdEntity("client6")),
                    Optional.empty()
            );
            checkQuota(clientQuotaManager, "userA", "client6", 12000, 4000, true); // Throttled due to sum of new and earlier values

        });
    }

    @Test
    public void testQuotaViolation() {
        withQuotaManager(QuotaType.PRODUCE, clientQuotaManager -> {
            KafkaMetric queueSizeMetric = metrics.metrics().get(metrics.metricName("queue-size", "Produce", ""));
            clientQuotaManager.updateQuota(
                    Optional.empty(),
                    Optional.of(ClientQuotaManager.DEFAULT_USER_CLIENT_ID),
                    Optional.of(new Quota(500, true))
            );

            // We have 10 seconds windows. Make sure that there is no quota violation
            // if we produce under the quota
            for (int i = 0; i < 10; i++) {
                assertEquals(0, maybeRecord(clientQuotaManager, "ANONYMOUS", "unknown", 400));
                time.sleep(1000);
            }
            assertEquals(0, ((Double) queueSizeMetric.metricValue()).intValue());

            // Create a spike.
            // 400*10 + 2000 + 300 = 6300/10.5 = 600 bytes per second.
            // (600 - quota)/quota*window-size = (600-500)/500*10.5 seconds = 2100
            // 10.5 seconds because the last window is half complete
            time.sleep(500);
            int throttleTime = maybeRecord(clientQuotaManager, "ANONYMOUS", "unknown", 2300);

            assertEquals(2100, throttleTime, "Should be throttled");
            throttle(clientQuotaManager, throttleTime, callback);
            assertEquals(1, ((Double) queueSizeMetric.metricValue()).intValue());
            // After a request is delayed, the callback cannot be triggered immediately
            clientQuotaManager.processThrottledChannelReaperDoWork();
            assertEquals(0, numCallbacks);
            time.sleep(throttleTime);

            // Callback can only be triggered after the delay time passes
            clientQuotaManager.processThrottledChannelReaperDoWork();
            assertEquals(0, ((Double) queueSizeMetric.metricValue()).intValue());
            assertEquals(1, numCallbacks);

            // Could continue to see delays until the bursty sample disappears
            for (int i = 0; i < 10; i++) {
                maybeRecord(clientQuotaManager, "ANONYMOUS", "unknown", 400);
                time.sleep(1000);
            }

            assertEquals(0, maybeRecord(clientQuotaManager, "ANONYMOUS", "unknown", 0),
                    "Should be unthrottled since bursty sample has rolled over");
        });
    }

    @Test
    public void testExpireThrottleTimeSensor() {
        withQuotaManager(QuotaType.PRODUCE, clientQuotaManager -> {
            clientQuotaManager.updateQuota(
                    Optional.empty(),
                    Optional.of(ClientQuotaManager.DEFAULT_USER_CLIENT_ID),
                    Optional.of(new Quota(500, true))
            );

            maybeRecord(clientQuotaManager, "ANONYMOUS", "client1", 100);
            // remove the throttle time sensor
            metrics.removeSensor("ProduceThrottleTime-:client1");
            // should not throw an exception even if the throttle time sensor does not exist.
            int throttleTime = maybeRecord(clientQuotaManager, "ANONYMOUS", "client1", 10000);
            assertTrue(throttleTime > 0, "Should be throttled");
            // the sensor should get recreated
            Sensor throttleTimeSensor = metrics.getSensor("ProduceThrottleTime-:client1");
            assertNotNull(throttleTimeSensor, "Throttle time sensor should exist");
        });
    }

    @Test
    public void testExpireQuotaSensors() {
        withQuotaManager(QuotaType.PRODUCE, clientQuotaManager -> {
            clientQuotaManager.updateQuota(
                    Optional.empty(),
                    Optional.of(ClientQuotaManager.DEFAULT_USER_CLIENT_ID),
                    Optional.of(new Quota(500, true))
            );

            maybeRecord(clientQuotaManager, "ANONYMOUS", "client1", 100);
            // remove all the sensors
            metrics.removeSensor("ProduceThrottleTime-:client1");
            metrics.removeSensor("Produce-ANONYMOUS:client1");
            // should not throw an exception
            int throttleTime = maybeRecord(clientQuotaManager, "ANONYMOUS", "client1", 10000);
            assertTrue(throttleTime > 0, "Should be throttled");

            // all the sensors should get recreated
            Sensor throttleTimeSensor = metrics.getSensor("ProduceThrottleTime-:client1");
            assertNotNull(throttleTimeSensor, "Throttle time sensor should exist");

            Sensor byteRateSensor = metrics.getSensor("Produce-:client1");
            assertNotNull(byteRateSensor, "Byte rate sensor should exist");
        });
    }

    @Test
    public void testClientIdNotSanitized() {
        withQuotaManager(QuotaType.PRODUCE, clientQuotaManager -> {
            String clientId = "client@#$%";
            clientQuotaManager.updateQuota(
                    Optional.empty(),
                    Optional.of(ClientQuotaManager.DEFAULT_USER_CLIENT_ID),
                    Optional.of(new Quota(500, true))
            );

            maybeRecord(clientQuotaManager, "ANONYMOUS", clientId, 100);

            // The metrics should use the raw client ID, even if the reporters internally sanitize them
            Sensor throttleTimeSensor = metrics.getSensor("ProduceThrottleTime-:" + clientId);
            assertNotNull(throttleTimeSensor, "Throttle time sensor should exist");

            Sensor byteRateSensor = metrics.getSensor("Produce-:" + clientId);
            assertNotNull(byteRateSensor, "Byte rate sensor should exist");
        });
    }

    @Test
    public void testQuotaTypesEnabledUpdatesWithDefaultCallback() {
        withQuotaManager(QuotaType.CONTROLLER_MUTATION, clientQuotaManager -> {
            assertEquals(ClientQuotaManager.NO_QUOTAS, clientQuotaManager.quotaTypesEnabled());
            assertFalse(clientQuotaManager.quotasEnabled());

            clientQuotaManager.updateQuota(Optional.empty(), Optional.of(new ClientQuotaManager.ClientIdEntity("client1")), Optional.of(new Quota(5, true)));
            assertEquals(ClientQuotaManager.CLIENT_ID_QUOTA_ENABLED, clientQuotaManager.quotaTypesEnabled());
            assertTrue(clientQuotaManager.quotasEnabled());

            clientQuotaManager.updateQuota(Optional.of(new ClientQuotaManager.UserEntity("userA")), Optional.empty(), Optional.of(new Quota(5, true)));
            assertEquals(ClientQuotaManager.USER_QUOTA_ENABLED | ClientQuotaManager.CLIENT_ID_QUOTA_ENABLED, clientQuotaManager.quotaTypesEnabled());
            assertTrue(clientQuotaManager.quotasEnabled());

            clientQuotaManager.updateQuota(Optional.empty(), Optional.of(new ClientQuotaManager.ClientIdEntity("client2")), Optional.of(new Quota(5, true)));
            assertEquals(ClientQuotaManager.USER_QUOTA_ENABLED | ClientQuotaManager.CLIENT_ID_QUOTA_ENABLED, clientQuotaManager.quotaTypesEnabled());
            assertTrue(clientQuotaManager.quotasEnabled());

            clientQuotaManager.updateQuota(Optional.of(new ClientQuotaManager.UserEntity("userB")), Optional.empty(), Optional.of(new Quota(5, true)));
            assertEquals(ClientQuotaManager.USER_QUOTA_ENABLED | ClientQuotaManager.CLIENT_ID_QUOTA_ENABLED, clientQuotaManager.quotaTypesEnabled());
            assertTrue(clientQuotaManager.quotasEnabled());

            clientQuotaManager.updateQuota(Optional.of(new ClientQuotaManager.UserEntity("userA")), Optional.of(new ClientQuotaManager.ClientIdEntity("client1")), Optional.of(new Quota(10, true)));
            assertEquals(ClientQuotaManager.USER_CLIENT_ID_QUOTA_ENABLED | ClientQuotaManager.CLIENT_ID_QUOTA_ENABLED | ClientQuotaManager.USER_QUOTA_ENABLED, clientQuotaManager.quotaTypesEnabled());
            assertTrue(clientQuotaManager.quotasEnabled());

            clientQuotaManager.updateQuota(Optional.of(new ClientQuotaManager.UserEntity("userA")), Optional.of(new ClientQuotaManager.ClientIdEntity("client1")), Optional.of(new Quota(12, true)));
            assertEquals(ClientQuotaManager.USER_CLIENT_ID_QUOTA_ENABLED | ClientQuotaManager.CLIENT_ID_QUOTA_ENABLED | ClientQuotaManager.USER_QUOTA_ENABLED, clientQuotaManager.quotaTypesEnabled());
            assertTrue(clientQuotaManager.quotasEnabled());

            clientQuotaManager.updateQuota(Optional.of(new ClientQuotaManager.UserEntity("userA")), Optional.empty(), Optional.empty());
            assertEquals(ClientQuotaManager.USER_CLIENT_ID_QUOTA_ENABLED | ClientQuotaManager.CLIENT_ID_QUOTA_ENABLED | ClientQuotaManager.USER_QUOTA_ENABLED, clientQuotaManager.quotaTypesEnabled());
            assertTrue(clientQuotaManager.quotasEnabled());

            clientQuotaManager.updateQuota(Optional.of(new ClientQuotaManager.UserEntity("userB")), Optional.empty(), Optional.empty());
            assertEquals(ClientQuotaManager.USER_CLIENT_ID_QUOTA_ENABLED | ClientQuotaManager.CLIENT_ID_QUOTA_ENABLED, clientQuotaManager.quotaTypesEnabled());
            assertTrue(clientQuotaManager.quotasEnabled());

            clientQuotaManager.updateQuota(Optional.empty(), Optional.of(new ClientQuotaManager.ClientIdEntity("client1")), Optional.empty());
            assertEquals(ClientQuotaManager.USER_CLIENT_ID_QUOTA_ENABLED | ClientQuotaManager.CLIENT_ID_QUOTA_ENABLED, clientQuotaManager.quotaTypesEnabled());
            assertTrue(clientQuotaManager.quotasEnabled());

            clientQuotaManager.updateQuota(Optional.empty(), Optional.of(new ClientQuotaManager.ClientIdEntity("client2")), Optional.empty());
            assertEquals(ClientQuotaManager.USER_CLIENT_ID_QUOTA_ENABLED, clientQuotaManager.quotaTypesEnabled());
            assertTrue(clientQuotaManager.quotasEnabled());

            clientQuotaManager.updateQuota(Optional.of(new ClientQuotaManager.UserEntity("userA")), Optional.of(new ClientQuotaManager.ClientIdEntity("client1")), Optional.empty());
            assertEquals(ClientQuotaManager.NO_QUOTAS, clientQuotaManager.quotaTypesEnabled());
            assertFalse(clientQuotaManager.quotasEnabled());

            clientQuotaManager.updateQuota(Optional.of(new ClientQuotaManager.UserEntity("userA")), Optional.of(new ClientQuotaManager.ClientIdEntity("client1")), Optional.empty());
            assertEquals(ClientQuotaManager.NO_QUOTAS, clientQuotaManager.quotaTypesEnabled());
            assertFalse(clientQuotaManager.quotasEnabled());
        });
    }

    @Test
    public void testQuotaTypesEnabledUpdatesWithCustomCallback() {
        ClientQuotaCallback customQuotaCallback = new ClientQuotaCallback() {
            final Map<ClientQuotaEntity, Quota> quotas = new HashMap<>();
            @Override
            public void configure(Map<String, ?> configs) {}

            @Override
            public Map<String, String> quotaMetricTags(ClientQuotaType quotaType, KafkaPrincipal principal, String clientId) {
                return Map.of();
            }

            @Override
            public Double quotaLimit(ClientQuotaType quotaType, Map<String, String> metricTags) {
                return 1.0;
            }

            @Override
            public void updateQuota(ClientQuotaType quotaType, ClientQuotaEntity entity, double newValue) {
                quotas.put(entity, new Quota(newValue, true));
            }

            @Override
            public void removeQuota(ClientQuotaType quotaType, ClientQuotaEntity entity) {
                quotas.remove(entity);
            }

            @Override
            public boolean quotaResetRequired(ClientQuotaType quotaType) {
                return false;
            }

            @Override
            public void close() {}
        };
        ClientQuotaManager clientQuotaManager = new ClientQuotaManager(
                new ClientQuotaManagerConfig(),
                metrics,
                QuotaType.CONTROLLER_MUTATION,
                time,
                "",
                Optional.of(Plugin.wrapInstance(customQuotaCallback, metrics, ""))
        );

        try {
            assertEquals(ClientQuotaManager.CUSTOM_QUOTAS, clientQuotaManager.quotaTypesEnabled());
            assertTrue(clientQuotaManager.quotasEnabled(), "quotasEnabled should be true with custom callback");

            clientQuotaManager.updateQuota(Optional.empty(), Optional.of(new ClientQuotaManager.ClientIdEntity("client1")), Optional.of(new Quota(12, true)));
            assertEquals(ClientQuotaManager.CUSTOM_QUOTAS, clientQuotaManager.quotaTypesEnabled());

            clientQuotaManager.updateQuota(Optional.of(new ClientQuotaManager.UserEntity("userA")), Optional.empty(), Optional.of(new Quota(12, true)));
            assertEquals(ClientQuotaManager.CUSTOM_QUOTAS, clientQuotaManager.quotaTypesEnabled());
            assertTrue(clientQuotaManager.quotasEnabled(), "quotasEnabled should remain true");

            clientQuotaManager.updateQuota(Optional.of(new ClientQuotaManager.UserEntity("userA")), Optional.of(new ClientQuotaManager.ClientIdEntity("client1")), Optional.of(new Quota(12, true)));
            assertEquals(ClientQuotaManager.CUSTOM_QUOTAS, clientQuotaManager.quotaTypesEnabled());
            assertTrue(clientQuotaManager.quotasEnabled(), "quotasEnabled should remain true");

            clientQuotaManager.updateQuota(Optional.of(new ClientQuotaManager.UserEntity("userA")), Optional.of(new ClientQuotaManager.ClientIdEntity("client1")), Optional.empty());
            clientQuotaManager.updateQuota(Optional.of(new ClientQuotaManager.UserEntity("userA")), Optional.empty(), Optional.empty());
            clientQuotaManager.updateQuota(Optional.empty(), Optional.of(new ClientQuotaManager.ClientIdEntity("client1")), Optional.empty());
            assertEquals(ClientQuotaManager.CUSTOM_QUOTAS, clientQuotaManager.quotaTypesEnabled());
            assertTrue(clientQuotaManager.quotasEnabled(), "quotasEnabled should remain true");
        } finally {
            clientQuotaManager.shutdown();
        }
    }

    record UserClient(
            String user,
            String clientId,
            Optional<ClientQuotaEntity.ConfigEntity> configUser,
            Optional<ClientQuotaEntity.ConfigEntity> configClientEntity) { }

}
