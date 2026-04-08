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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterClientQuotasOptions;
import org.apache.kafka.clients.admin.AlterUserScramCredentialsResult;
import org.apache.kafka.clients.admin.ScramCredentialInfo;
import org.apache.kafka.clients.admin.ScramMechanism;
import org.apache.kafka.clients.admin.UserScramCredentialUpsertion;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.quota.ClientQuotaFilter;
import org.apache.kafka.common.quota.ClientQuotaFilterComponent;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.server.config.QuotaConfig;
import org.apache.kafka.test.TestUtils;

import java.net.InetAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ClientQuotasRequestTest {
    private final ClusterInstance cluster;

    public ClientQuotasRequestTest(ClusterInstance cluster) {
        this.cluster = cluster;
    }

    @ClusterTest
    public void testAlterClientQuotasRequest() throws InterruptedException {
        ClientQuotaEntity entity = new ClientQuotaEntity(
            Map.of(ClientQuotaEntity.USER, "user", ClientQuotaEntity.CLIENT_ID, "client-id"));

        // Expect an empty configuration.
        verifyDescribeEntityQuotas(entity, Map.of());

        // Add two configuration entries.
        alterEntityQuotas(entity, Map.of(
            QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, Optional.of(10000.0),
            QuotaConfig.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG, Optional.of(20000.0)
        ), false);

        verifyDescribeEntityQuotas(entity, Map.of(
            QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, 10000.0,
            QuotaConfig.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG, 20000.0
        ));

        // Update an existing entry.
        alterEntityQuotas(entity, Map.of(
            QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, Optional.of(15000.0)
        ), false);

        verifyDescribeEntityQuotas(entity, Map.of(
            QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, 15000.0,
            QuotaConfig.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG, 20000.0
        ));

        // Remove an existing configuration entry.
        alterEntityQuotas(entity, Map.of(
            QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, Optional.empty()
        ), false);

        verifyDescribeEntityQuotas(entity, Map.of(
            QuotaConfig.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG, 20000.0
        ));

        // Remove a non-existent configuration entry.  This should make no changes.
        alterEntityQuotas(entity, Map.of(
            QuotaConfig.REQUEST_PERCENTAGE_OVERRIDE_CONFIG, Optional.empty()
        ), false);

        verifyDescribeEntityQuotas(entity, Map.of(
            QuotaConfig.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG, 20000.0
        ));

        // Add back a deleted configuration entry.
        alterEntityQuotas(entity, Map.of(
            QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, Optional.of(5000.0)
        ), false);

        verifyDescribeEntityQuotas(entity, Map.of(
            QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, 5000.0,
            QuotaConfig.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG, 20000.0
        ));

        // Perform a mixed update.
        alterEntityQuotas(entity, Map.of(
            QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, Optional.of(20000.0),
            QuotaConfig.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG, Optional.empty(),
            QuotaConfig.REQUEST_PERCENTAGE_OVERRIDE_CONFIG, Optional.of(12.3)
        ), false);

        verifyDescribeEntityQuotas(entity, Map.of(
            QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, 20000.0,
            QuotaConfig.REQUEST_PERCENTAGE_OVERRIDE_CONFIG, 12.3
        ));
    }

    @ClusterTest
    public void testAlterClientQuotasRequestValidateOnly() throws InterruptedException {
        ClientQuotaEntity entity = new ClientQuotaEntity(Map.of(ClientQuotaEntity.USER, "user"));

        // Set up a configuration.
        alterEntityQuotas(entity, Map.of(
            QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, Optional.of(20000.0),
            QuotaConfig.REQUEST_PERCENTAGE_OVERRIDE_CONFIG, Optional.of(23.45)
        ), false);

        verifyDescribeEntityQuotas(entity, Map.of(
            QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, 20000.0,
            QuotaConfig.REQUEST_PERCENTAGE_OVERRIDE_CONFIG, 23.45
        ));

        // Validate-only addition.
        alterEntityQuotas(entity, Map.of(
            QuotaConfig.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG, Optional.of(50000.0)
        ), true);

        verifyDescribeEntityQuotas(entity, Map.of(
            QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, 20000.0,
            QuotaConfig.REQUEST_PERCENTAGE_OVERRIDE_CONFIG, 23.45
        ));

        // Validate-only modification.
        alterEntityQuotas(entity, Map.of(
            QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, Optional.of(10000.0)
        ), true);

        verifyDescribeEntityQuotas(entity, Map.of(
            QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, 20000.0,
            QuotaConfig.REQUEST_PERCENTAGE_OVERRIDE_CONFIG, 23.45
        ));

        // Validate-only removal.
        alterEntityQuotas(entity, Map.of(
            QuotaConfig.REQUEST_PERCENTAGE_OVERRIDE_CONFIG, Optional.empty()
        ), true);

        verifyDescribeEntityQuotas(entity, Map.of(
            QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, 20000.0,
            QuotaConfig.REQUEST_PERCENTAGE_OVERRIDE_CONFIG, 23.45
        ));

        // Validate-only mixed update.
        alterEntityQuotas(entity, Map.of(
            QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, Optional.of(10000.0),
            QuotaConfig.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG, Optional.of(50000.0),
            QuotaConfig.REQUEST_PERCENTAGE_OVERRIDE_CONFIG, Optional.empty()
        ), true);

        verifyDescribeEntityQuotas(entity, Map.of(
            QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, 20000.0,
            QuotaConfig.REQUEST_PERCENTAGE_OVERRIDE_CONFIG, 23.45
        ));
    }

    @ClusterTest
    public void testClientQuotasForScramUsers() throws InterruptedException, ExecutionException {
        final String userName = "user";

        try (Admin admin = cluster.admin()) {
            AlterUserScramCredentialsResult results = admin.alterUserScramCredentials(List.of(
                new UserScramCredentialUpsertion(userName, new ScramCredentialInfo(ScramMechanism.SCRAM_SHA_256, 4096), "password")));
            results.all().get();

            ClientQuotaEntity entity = new ClientQuotaEntity(Map.of(ClientQuotaEntity.USER, userName));

            verifyDescribeEntityQuotas(entity, Map.of());

            alterEntityQuotas(entity, Map.of(
                QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, Optional.of(10000.0),
                QuotaConfig.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG, Optional.of(20000.0)
            ), false);

            verifyDescribeEntityQuotas(entity, Map.of(
                QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, 10000.0,
                QuotaConfig.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG, 20000.0
            ));
        }
    }

    @ClusterTest
    public void testAlterIpQuotasRequest() throws InterruptedException {
        final String knownHost = "1.2.3.4";
        final String unknownHost = "2.3.4.5";
        ClientQuotaEntity entity = toIpEntity(Optional.of(knownHost));
        ClientQuotaEntity defaultEntity = toIpEntity(Optional.empty());
        ClientQuotaFilterComponent entityFilter = ClientQuotaFilterComponent.ofEntity(ClientQuotaEntity.IP, knownHost);
        ClientQuotaFilterComponent defaultEntityFilter = ClientQuotaFilterComponent.ofDefaultEntity(ClientQuotaEntity.IP);
        ClientQuotaFilterComponent allIpEntityFilter = ClientQuotaFilterComponent.ofEntityType(ClientQuotaEntity.IP);

        // Expect an empty configuration.
        verifyIpQuotas(allIpEntityFilter, Map.of(), unknownHost);

        // Add a configuration entry.
        alterEntityQuotas(entity, Map.of(QuotaConfig.IP_CONNECTION_RATE_OVERRIDE_CONFIG, Optional.of(100.0)), false);
        verifyIpQuotas(entityFilter, Map.of(entity, 100.0), unknownHost);

        // update existing entry
        alterEntityQuotas(entity, Map.of(QuotaConfig.IP_CONNECTION_RATE_OVERRIDE_CONFIG, Optional.of(150.0)), false);
        verifyIpQuotas(entityFilter, Map.of(entity, 150.0), unknownHost);

        // update default value
        alterEntityQuotas(defaultEntity, Map.of(QuotaConfig.IP_CONNECTION_RATE_OVERRIDE_CONFIG, Optional.of(200.0)), false);
        verifyIpQuotas(defaultEntityFilter, Map.of(defaultEntity, 200.0), unknownHost);

        // describe all IP quotas
        verifyIpQuotas(allIpEntityFilter, Map.of(entity, 150.0, defaultEntity, 200.0), unknownHost);

        // remove entry
        alterEntityQuotas(entity, Map.of(QuotaConfig.IP_CONNECTION_RATE_OVERRIDE_CONFIG, Optional.empty()), false);
        verifyIpQuotas(entityFilter, Map.of(), unknownHost);

        // remove default value
        alterEntityQuotas(defaultEntity, Map.of(QuotaConfig.IP_CONNECTION_RATE_OVERRIDE_CONFIG, Optional.empty()), false);
        verifyIpQuotas(allIpEntityFilter, Map.of(), unknownHost);
    }

    private void verifyIpQuotas(ClientQuotaFilterComponent entityFilter, Map<ClientQuotaEntity, Double> expectedMatches,
        String unknownHost) throws InterruptedException {

        TestUtils.retryOnExceptionWithTimeout(5000L, () -> {
            Map<ClientQuotaEntity, Map<String, Double>> result = describeClientQuotas(
                ClientQuotaFilter.containsOnly(List.of(entityFilter))).get();
            assertEquals(expectedMatches.keySet(), result.keySet());

            for (Map.Entry<ClientQuotaEntity, Map<String, Double>> entry : result.entrySet()) {
                ClientQuotaEntity entity = entry.getKey();
                Map<String, Double> props = entry.getValue();
                assertEquals(Set.of(QuotaConfig.IP_CONNECTION_RATE_OVERRIDE_CONFIG), props.keySet());
                assertEquals(expectedMatches.get(entity), props.get(QuotaConfig.IP_CONNECTION_RATE_OVERRIDE_CONFIG));
                String entityName = entity.entries().get(ClientQuotaEntity.IP);
                // ClientQuotaEntity with null name maps to default entity
                InetAddress entityIp = entityName == null
                    ? InetAddress.getByName(unknownHost)
                    : InetAddress.getByName(entityName);
                int currentServerQuota = cluster.brokers()
                    .values()
                    .iterator()
                    .next()
                    .socketServer()
                    .connectionQuotas()
                    .connectionRateForIp(entityIp);
                assertTrue(Math.abs(expectedMatches.get(entity) - currentServerQuota) < 0.01,
                    String.format("Connection quota of %s is not %s but %s", entity, expectedMatches.get(entity), currentServerQuota));
            }
        });
    }

    @ClusterTest
    public void testAlterClientQuotasInvalidRequests() {
        final ClientQuotaEntity entity1 = new ClientQuotaEntity(Map.of(ClientQuotaEntity.USER, ""));
        TestUtils.assertFutureThrows(InvalidRequestException.class,
            alterEntityQuotas(entity1, Map.of(QuotaConfig.REQUEST_PERCENTAGE_OVERRIDE_CONFIG, Optional.of(12.34)), true));

        final ClientQuotaEntity entity2 = new ClientQuotaEntity(Map.of(ClientQuotaEntity.CLIENT_ID, ""));
        TestUtils.assertFutureThrows(InvalidRequestException.class,
            alterEntityQuotas(entity2, Map.of(QuotaConfig.REQUEST_PERCENTAGE_OVERRIDE_CONFIG, Optional.of(12.34)), true));

        final ClientQuotaEntity entity3 = new ClientQuotaEntity(Map.of("", "name"));
        TestUtils.assertFutureThrows(InvalidRequestException.class,
            alterEntityQuotas(entity3, Map.of(QuotaConfig.REQUEST_PERCENTAGE_OVERRIDE_CONFIG, Optional.of(12.34)), true));

        final ClientQuotaEntity entity4 = new ClientQuotaEntity(Map.of());
        TestUtils.assertFutureThrows(InvalidRequestException.class,
            alterEntityQuotas(entity4, Map.of(QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, Optional.of(10000.5)), true));

        final ClientQuotaEntity entity5 = new ClientQuotaEntity(Map.of(ClientQuotaEntity.USER, "user"));
        TestUtils.assertFutureThrows(InvalidRequestException.class,
            alterEntityQuotas(entity5, Map.of("bad", Optional.of(1.0)), true));

        final ClientQuotaEntity entity6 = new ClientQuotaEntity(Map.of(ClientQuotaEntity.USER, "user"));
        TestUtils.assertFutureThrows(InvalidRequestException.class,
            alterEntityQuotas(entity6, Map.of(QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, Optional.of(10000.5)), true));
    }

    private void expectInvalidRequestWithMessage(Future<?> future, String expectedMessage) {
        InvalidRequestException exception = TestUtils.assertFutureThrows(InvalidRequestException.class, future);
        assertNotNull(exception);
        assertTrue(
            exception.getMessage().contains(expectedMessage),
            String.format("Expected message %s to contain %s", exception, expectedMessage)
        );
    }

    @ClusterTest
    public void testAlterClientQuotasInvalidEntityCombination() {
        ClientQuotaEntity userAndIpEntity = new ClientQuotaEntity(
            Map.of(ClientQuotaEntity.USER, "user", ClientQuotaEntity.IP, "1.2.3.4")
        );
        ClientQuotaEntity clientAndIpEntity = new ClientQuotaEntity(
            Map.of(ClientQuotaEntity.CLIENT_ID, "client", ClientQuotaEntity.IP, "1.2.3.4")
        );
        final String expectedExceptionMessage = "Invalid quota entity combination";

        expectInvalidRequestWithMessage(
            alterEntityQuotas(userAndIpEntity, Map.of(QuotaConfig.REQUEST_PERCENTAGE_OVERRIDE_CONFIG, Optional.of(12.34)), true),
            expectedExceptionMessage
        );

        expectInvalidRequestWithMessage(
            alterEntityQuotas(clientAndIpEntity, Map.of(QuotaConfig.REQUEST_PERCENTAGE_OVERRIDE_CONFIG, Optional.of(12.34)), true),
            expectedExceptionMessage
        );
    }

    @ClusterTest
    public void testAlterClientQuotasBadIp() {
        ClientQuotaEntity invalidHostPatternEntity = new ClientQuotaEntity(
            Map.of(ClientQuotaEntity.IP, "not a valid host because it has spaces")
        );
        ClientQuotaEntity unresolvableHostEntity = new ClientQuotaEntity(
            Map.of(ClientQuotaEntity.IP, "RFC2606.invalid")
        );
        final String expectedExceptionMessage = "not a valid IP";

        expectInvalidRequestWithMessage(
            alterEntityQuotas(invalidHostPatternEntity, Map.of(QuotaConfig.IP_CONNECTION_RATE_OVERRIDE_CONFIG, Optional.of(50.0)), true),
            expectedExceptionMessage
        );

        expectInvalidRequestWithMessage(
            alterEntityQuotas(unresolvableHostEntity, Map.of(QuotaConfig.IP_CONNECTION_RATE_OVERRIDE_CONFIG, Optional.of(50.0)), true),
            expectedExceptionMessage
        );
    }

    @ClusterTest
    public void testDescribeClientQuotasInvalidFilterCombination() {
        ClientQuotaFilterComponent ipFilterComponent = ClientQuotaFilterComponent.ofEntityType(ClientQuotaEntity.IP);
        ClientQuotaFilterComponent userFilterComponent = ClientQuotaFilterComponent.ofEntityType(ClientQuotaEntity.USER);
        ClientQuotaFilterComponent clientIdFilterComponent = ClientQuotaFilterComponent.ofEntityType(ClientQuotaEntity.CLIENT_ID);
        final String expectedExceptionMessage = "Invalid entity filter component combination";

        expectInvalidRequestWithMessage(
            describeClientQuotas(ClientQuotaFilter.contains(List.of(ipFilterComponent, userFilterComponent))),
            expectedExceptionMessage
        );
        expectInvalidRequestWithMessage(
            describeClientQuotas(ClientQuotaFilter.contains(List.of(ipFilterComponent, clientIdFilterComponent))),
            expectedExceptionMessage
        );
    }

    // Entities to be matched against.
    private final Map<ClientQuotaEntity, Double> matchUserClientEntities = new HashMap<>(Map.ofEntries(
        Map.entry(toClientEntity(toUserMap("user-1"), toClientIdMap("client-id-1")), 50.50),
        Map.entry(toClientEntity(toUserMap("user-2"), toClientIdMap("client-id-1")), 51.51),
        Map.entry(toClientEntity(toUserMap("user-3"), toClientIdMap("client-id-2")), 52.52),
        Map.entry(toClientEntity(toUserMap(null), toClientIdMap("client-id-1")), 53.53),
        Map.entry(toClientEntity(toUserMap("user-1"), toClientIdMap(null)), 54.54),
        Map.entry(toClientEntity(toUserMap("user-3"), toClientIdMap(null)), 55.55),
        Map.entry(toClientEntity(toUserMap("user-1")), 56.56),
        Map.entry(toClientEntity(toUserMap("user-2")), 57.57),
        Map.entry(toClientEntity(toUserMap("user-3")), 58.58),
        Map.entry(toClientEntity(toUserMap(null)), 59.59),
        Map.entry(toClientEntity(toClientIdMap("client-id-2")), 60.60)
    ));

    private final Map<ClientQuotaEntity, Double> matchIpEntities = Map.of(
        toIpEntity(Optional.of("1.2.3.4")), 10.0,
        toIpEntity(Optional.of("2.3.4.5")), 20.0
    );

    private void setupDescribeClientQuotasMatchTest() {
        Map<ClientQuotaEntity, Map<String, Optional<Double>>> userClientQuotas = matchUserClientEntities.entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey,
                e -> Map.of(QuotaConfig.REQUEST_PERCENTAGE_OVERRIDE_CONFIG, Optional.of(e.getValue()))));

        Map<ClientQuotaEntity, Map<String, Optional<Double>>> ipQuotas = matchIpEntities.entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey,
                e -> Map.of(QuotaConfig.IP_CONNECTION_RATE_OVERRIDE_CONFIG, Optional.of(e.getValue()))));

        Map<ClientQuotaEntity, Map<String, Optional<Double>>> allQuotas = new HashMap<>();
        allQuotas.putAll(userClientQuotas);
        allQuotas.putAll(ipQuotas);
        Map<ClientQuotaEntity, KafkaFuture<Void>> result = alterClientQuotas(allQuotas, false);

        matchUserClientEntities.forEach((entity, value) -> {
            try {
                result.get(entity).get(10, TimeUnit.SECONDS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                throw new RuntimeException(e);
            }
        });
        matchIpEntities.forEach((entity, value) -> {
            try {
                result.get(entity).get(10, TimeUnit.SECONDS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private Map<ClientQuotaEntity, Map<String, Double>> matchEntity(ClientQuotaEntity entity)
        throws ExecutionException, InterruptedException {
        List<ClientQuotaFilterComponent> components = entity.entries().entrySet().stream().map(entry -> {
            if (entry.getValue() == null) {
                return ClientQuotaFilterComponent.ofDefaultEntity(entry.getKey());
            } else {
                return ClientQuotaFilterComponent.ofEntity(entry.getKey(), entry.getValue());
            }
        }).toList();

        return describeClientQuotas(ClientQuotaFilter.containsOnly(components)).get();
    }

    @ClusterTest
    public void testDescribeClientQuotasMatchExact() throws ExecutionException, InterruptedException {
        setupDescribeClientQuotasMatchTest();

        // Test exact matches.
        matchUserClientEntities.forEach((e, v) -> {
            try {
                TestUtils.retryOnExceptionWithTimeout(5000L, () -> {
                    Map<ClientQuotaEntity, Map<String, Double>> result = matchEntity(e);
                    assertEquals(1, result.size());
                    assertNotNull(result.get(e));
                    double value = result.get(e).get(QuotaConfig.REQUEST_PERCENTAGE_OVERRIDE_CONFIG);
                    assertEquals(value, v, 1e-6);
                });
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            }
        });

        // Entities not contained in `matchEntityList`.
        List<ClientQuotaEntity> notMatchEntities = List.of(
            toClientEntity(toUserMap("user-1"), toClientIdMap("client-id-2")),
            toClientEntity(toUserMap("user-3"), toClientIdMap("client-id-1")),
            toClientEntity(toUserMap("user-2"), toClientIdMap(null)),
            toClientEntity(toUserMap("user-4")),
            toClientEntity(toUserMap(null), toClientIdMap("client-id-2")),
            toClientEntity(toClientIdMap("client-id-1")),
            toClientEntity(toClientIdMap("client-id-3"))
        );

        // Verify exact matches of the non-matches returns empty.
        for (ClientQuotaEntity e : notMatchEntities) {
            Map<ClientQuotaEntity, Map<String, Double>> result = matchEntity(e);
            assertEquals(0, result.size());
        }
    }

    @SuppressWarnings("unchecked")
    private void testMatchEntities(ClientQuotaFilter filter, int expectedMatchSize, Predicate<ClientQuotaEntity> partition)
        throws InterruptedException {
        TestUtils.retryOnExceptionWithTimeout(5000L, () -> {
            Map<ClientQuotaEntity, Map<String, Double>> result = describeClientQuotas(filter).get();
            List<Map.Entry<ClientQuotaEntity, Double>> expectedMatches = matchUserClientEntities.entrySet()
                .stream()
                .collect(Collectors.partitioningBy(entry -> partition.test(entry.getKey())))
                .get(true);
            expectedMatches.addAll(matchIpEntities.entrySet()
                .stream()
                .collect(Collectors.partitioningBy(entry -> partition.test(entry.getKey())))
                .get(true));

            // for test verification
            assertEquals(expectedMatchSize, expectedMatches.size());
            assertEquals(expectedMatchSize, result.size(),
                "Failed to match " + expectedMatchSize + "entities for " + filter);
            Map<Object, Object> expectedMatchesMap = Map.ofEntries(expectedMatches.toArray(new Map.Entry[0]));
            matchUserClientEntities.forEach((entity, expectedValue) -> {
                if (expectedMatchesMap.containsKey(entity)) {
                    Map<String, Double> config = result.get(entity);
                    assertNotNull(config);
                    Double value = config.get(QuotaConfig.REQUEST_PERCENTAGE_OVERRIDE_CONFIG);
                    assertNotNull(value);
                    assertEquals(expectedValue, value, 1e-6);
                } else {
                    assertNull(result.get(entity));
                }
            });
            matchIpEntities.forEach((entity, expectedValue) -> {
                if (expectedMatchesMap.containsKey(entity)) {
                    Map<String, Double> config = result.get(entity);
                    assertNotNull(config);
                    Double value = config.get(QuotaConfig.IP_CONNECTION_RATE_OVERRIDE_CONFIG);
                    assertNotNull(value);
                    assertEquals(expectedValue, value, 1e-6);
                } else {
                    assertNull(result.get(entity));
                }
            });
        });
    }

    @ClusterTest
    public void testDescribeClientQuotasMatchPartial() throws InterruptedException {
        setupDescribeClientQuotasMatchTest();

        // Match open-ended existing user.
        testMatchEntities(
            ClientQuotaFilter.contains(List.of(ClientQuotaFilterComponent.ofEntity(ClientQuotaEntity.USER, "user-1"))),
            3,
            entity -> Objects.equals(entity.entries().get(ClientQuotaEntity.USER), "user-1")
        );

        // Match open-ended non-existent user.
        testMatchEntities(
            ClientQuotaFilter.contains(List.of(ClientQuotaFilterComponent.ofEntity(ClientQuotaEntity.USER, "unknown"))),
            0,
            entity -> false
        );

        // Match open-ended existing client ID.
        testMatchEntities(
            ClientQuotaFilter.contains(List.of(ClientQuotaFilterComponent.ofEntity(ClientQuotaEntity.CLIENT_ID, "client-id-2"))),
            2,
            entity -> Objects.equals(entity.entries().get(ClientQuotaEntity.CLIENT_ID), "client-id-2")
        );

        // Match open-ended default user.
        testMatchEntities(
            ClientQuotaFilter.contains(List.of(ClientQuotaFilterComponent.ofDefaultEntity(ClientQuotaEntity.USER))),
            2,
            entity -> entity.entries().containsKey(ClientQuotaEntity.USER) && entity.entries().get(ClientQuotaEntity.USER) == null
        );

        // Match close-ended existing user.
        testMatchEntities(
            ClientQuotaFilter.containsOnly(List.of(ClientQuotaFilterComponent.ofEntity(ClientQuotaEntity.USER, "user-2"))),
            1,
            entity -> Objects.equals(entity.entries().get(ClientQuotaEntity.USER), "user-2") && !entity.entries().containsKey(ClientQuotaEntity.CLIENT_ID)
        );

        // Match close-ended existing client ID that has no matching entity.
        testMatchEntities(
            ClientQuotaFilter.containsOnly(List.of(ClientQuotaFilterComponent.ofEntity(ClientQuotaEntity.CLIENT_ID, "client-id-1"))),
            0,
            entity -> false
        );

        // Match against all entities with the user type in a close-ended match.
        testMatchEntities(
            ClientQuotaFilter.containsOnly(List.of(ClientQuotaFilterComponent.ofEntityType(ClientQuotaEntity.USER))),
            4,
            entity -> entity.entries().containsKey(ClientQuotaEntity.USER) && !entity.entries().containsKey(ClientQuotaEntity.CLIENT_ID)
        );

        // Match against all entities with the user type in an open-ended match.
        testMatchEntities(
            ClientQuotaFilter.contains(List.of(ClientQuotaFilterComponent.ofEntityType(ClientQuotaEntity.USER))),
            10,
            entity -> entity.entries().containsKey(ClientQuotaEntity.USER)
        );

        // Match against all entities with the client ID type in a close-ended match.
        testMatchEntities(
            ClientQuotaFilter.containsOnly(List.of(ClientQuotaFilterComponent.ofEntityType(ClientQuotaEntity.CLIENT_ID))),
            1,
            entity -> entity.entries().containsKey(ClientQuotaEntity.CLIENT_ID) && !entity.entries().containsKey(ClientQuotaEntity.USER)
        );

        // Match against all entities with the client ID type in an open-ended match.
        testMatchEntities(
            ClientQuotaFilter.contains(List.of(ClientQuotaFilterComponent.ofEntityType(ClientQuotaEntity.CLIENT_ID))),
            7,
            entity -> entity.entries().containsKey(ClientQuotaEntity.CLIENT_ID)
        );

        // Match against all entities with IP type in an open-ended match.
        testMatchEntities(
            ClientQuotaFilter.contains(List.of(ClientQuotaFilterComponent.ofEntityType(ClientQuotaEntity.IP))),
            2,
            entity -> entity.entries().containsKey(ClientQuotaEntity.IP)
        );

        // Match open-ended empty filter List. This should match all entities.
        testMatchEntities(ClientQuotaFilter.contains(List.of()), 13, entity -> true);

        // Match close-ended empty filter List. This should match no entities.
        testMatchEntities(ClientQuotaFilter.containsOnly(List.of()), 0, entity -> false);
    }

    @ClusterTest
    public void testClientQuotasUnsupportedEntityTypes() {
        ClientQuotaEntity entity = new ClientQuotaEntity(Map.of("other", "name"));
        KafkaFuture<Map<ClientQuotaEntity, Map<String, Double>>> future = describeClientQuotas(
            ClientQuotaFilter.containsOnly(getComponents(entity)));

        TestUtils.assertFutureThrows(UnsupportedVersionException.class, future);
    }

    @ClusterTest
    public void testClientQuotasSanitized() throws InterruptedException {
        // An entity with name that must be sanitized when writing to Zookeeper.
        ClientQuotaEntity entity = new ClientQuotaEntity(Map.of(ClientQuotaEntity.USER, "user with spaces"));

        alterEntityQuotas(entity, Map.of(
            QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, Optional.of(20000.0)
        ), false);

        verifyDescribeEntityQuotas(entity, Map.of(
            QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, 20000.0
        ));
    }

    private Map<String, String> toUserMap(String user) {
        // Uses Collections.singletonMap instead of Map.of to support null user parameter.
        return Collections.singletonMap(ClientQuotaEntity.USER, user);
    }

    private Map<String, String> toClientIdMap(String clientId) {
        // Uses Collections.singletonMap instead of Map.of to support null client-id parameter.
        return Collections.singletonMap(ClientQuotaEntity.CLIENT_ID, clientId);
    }

    @SafeVarargs
    private ClientQuotaEntity toClientEntity(Map<String, String>... entries) {
        Map<String, String> entityMap = new HashMap<>();
        for (Map<String, String> entry : entries) {
            entityMap.putAll(entry);
        }
        return new ClientQuotaEntity(entityMap);
    }

    private ClientQuotaEntity toIpEntity(Optional<String> ip) {
        return new ClientQuotaEntity(Collections.singletonMap(ClientQuotaEntity.IP, ip.orElse(null)));
    }

    private void verifyDescribeEntityQuotas(ClientQuotaEntity entity, Map<String, Double> quotas)
        throws InterruptedException {
        TestUtils.retryOnExceptionWithTimeout(5000L, () -> {

            Map<ClientQuotaEntity, Map<String, Double>> describe = describeClientQuotas(
                ClientQuotaFilter.containsOnly(getComponents(entity))).get();
            if (quotas.isEmpty()) {
                assertEquals(0, describe.size());
            } else {
                assertEquals(1, describe.size());
                Map<String, Double> configs = describe.get(entity);
                assertNotNull(configs);
                assertEquals(quotas.size(), configs.size());

                quotas.forEach((k, v) -> {
                    Double value = configs.get(k);
                    assertNotNull(value);
                    assertEquals(v, value, 1e-6);
                });
            }
        });
    }

    private List<ClientQuotaFilterComponent> getComponents(ClientQuotaEntity entity) {
        return entity.entries().entrySet().stream().map(entry -> {
            String entityType = entry.getKey();
            String entityName = entry.getValue();
            return Optional.ofNullable(entityName)
                .map(name -> ClientQuotaFilterComponent.ofEntity(entityType, name))
                .orElseGet(() -> ClientQuotaFilterComponent.ofDefaultEntity(entityType));
        }).toList();
    }

    private KafkaFuture<Map<ClientQuotaEntity, Map<String, Double>>> describeClientQuotas(ClientQuotaFilter filter) {
        try (Admin admin = cluster.admin()) {
            return admin.describeClientQuotas(filter).entities();
        }
    }

    private KafkaFuture<Void> alterEntityQuotas(ClientQuotaEntity entity, Map<String, Optional<Double>> alter, boolean validateOnly) {

        return alterClientQuotas(Map.of(entity, alter), validateOnly).get(entity);
    }

    private Map<ClientQuotaEntity, KafkaFuture<Void>> alterClientQuotas(Map<ClientQuotaEntity, Map<String,
        Optional<Double>>> request, boolean validateOnly) {

        List<ClientQuotaAlteration> entries = request.entrySet().stream().map(entry -> {
            ClientQuotaEntity entity = entry.getKey();
            Map<String, Optional<Double>> alter = entry.getValue();

            List<ClientQuotaAlteration.Op> ops = alter.entrySet()
                .stream()
                .map(configEntry -> new ClientQuotaAlteration.Op(configEntry.getKey(),
                    configEntry.getValue().orElse(null)))
                .toList();
            return new ClientQuotaAlteration(entity, ops);
        }).toList();

        try (Admin admin = cluster.admin()) {
            Map<ClientQuotaEntity, KafkaFuture<Void>> result = admin.alterClientQuotas(entries,
                new AlterClientQuotasOptions().validateOnly(validateOnly)).values();
            assertEquals(request.size(), result.size());
            request.forEach((e, r) -> assertTrue(result.containsKey(e)));
            return result;
        }
    }
}
