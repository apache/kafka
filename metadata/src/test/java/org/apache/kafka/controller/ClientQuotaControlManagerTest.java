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

package org.apache.kafka.controller;

import org.apache.kafka.common.config.internals.QuotaConfigs;
import org.apache.kafka.common.metadata.QuotaRecord;
import org.apache.kafka.common.metadata.QuotaRecord.EntityData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.metadata.ApiMessageAndVersion;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(value = 40)
public class ClientQuotaControlManagerTest {

    @Test
    public void testInvalidEntityTypes() {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(new LogContext());
        ClientQuotaControlManager manager = new ClientQuotaControlManager(snapshotRegistry);

        // Unknown type "foo"
        assertInvalidEntity(manager, entity("foo", "bar"));

        // Null type
        assertInvalidEntity(manager, entity(null, "null"));

        // Valid + unknown combo
        assertInvalidEntity(manager, entity(ClientQuotaEntity.USER, "user-1", "foo", "bar"));
        assertInvalidEntity(manager, entity("foo", "bar", ClientQuotaEntity.IP, "1.2.3.4"));

        // Invalid combinations
        assertInvalidEntity(manager, entity(ClientQuotaEntity.USER, "user-1", ClientQuotaEntity.IP, "1.2.3.4"));
        assertInvalidEntity(manager, entity(ClientQuotaEntity.CLIENT_ID, "user-1", ClientQuotaEntity.IP, "1.2.3.4"));

        // Empty
        assertInvalidEntity(manager, new ClientQuotaEntity(Collections.emptyMap()));
    }

    private void assertInvalidEntity(ClientQuotaControlManager manager, ClientQuotaEntity entity) {
        assertInvalidQuota(manager, entity, quotas(QuotaConfigs.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, 10000.0));
    }

    @Test
    public void testInvalidQuotaKeys() {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(new LogContext());
        ClientQuotaControlManager manager = new ClientQuotaControlManager(snapshotRegistry);
        ClientQuotaEntity entity = entity(ClientQuotaEntity.USER, "user-1");

        // Invalid + valid keys
        assertInvalidQuota(manager, entity, quotas("not.a.quota.key", 0.0, QuotaConfigs.REQUEST_PERCENTAGE_OVERRIDE_CONFIG, 99.9));

        // Valid + invalid keys
        assertInvalidQuota(manager, entity, quotas(QuotaConfigs.REQUEST_PERCENTAGE_OVERRIDE_CONFIG, 99.9, "not.a.quota.key", 0.0));

        // Null key
        assertInvalidQuota(manager, entity, quotas(null, 99.9));
    }

    private void assertInvalidQuota(ClientQuotaControlManager manager, ClientQuotaEntity entity, Map<String, Double> quota) {
        List<ClientQuotaAlteration> alters = new ArrayList<>();
        entityQuotaToAlterations(entity, quota, alters::add);
        ControllerResult<Map<ClientQuotaEntity, ApiError>> result = manager.alterClientQuotas(alters);
        assertEquals(Errors.INVALID_REQUEST, result.response().get(entity).error());
        assertEquals(0, result.records().size());
    }

    @Test
    public void testAlterAndRemove() {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(new LogContext());
        ClientQuotaControlManager manager = new ClientQuotaControlManager(snapshotRegistry);

        ClientQuotaEntity userEntity = userEntity("user-1");
        List<ClientQuotaAlteration> alters = new ArrayList<>();

        // Add one quota
        entityQuotaToAlterations(userEntity, quotas(QuotaConfigs.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, 10000.0), alters::add);
        alterQuotas(alters, manager);
        assertEquals(1, manager.clientQuotaData.get(userEntity).size());
        assertEquals(10000.0, manager.clientQuotaData.get(userEntity).get(QuotaConfigs.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG), 1e-6);

        // Replace it and add another
        alters.clear();
        entityQuotaToAlterations(userEntity, quotas(
            QuotaConfigs.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, 10001.0,
            QuotaConfigs.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG, 20000.0
        ), alters::add);
        alterQuotas(alters, manager);
        assertEquals(2, manager.clientQuotaData.get(userEntity).size());
        assertEquals(10001.0, manager.clientQuotaData.get(userEntity).get(QuotaConfigs.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG), 1e-6);
        assertEquals(20000.0, manager.clientQuotaData.get(userEntity).get(QuotaConfigs.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG), 1e-6);

        // Remove one of the quotas, the other remains
        alters.clear();
        entityQuotaToAlterations(userEntity, quotas(
            QuotaConfigs.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, null
        ), alters::add);
        alterQuotas(alters, manager);
        assertEquals(1, manager.clientQuotaData.get(userEntity).size());
        assertEquals(20000.0, manager.clientQuotaData.get(userEntity).get(QuotaConfigs.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG), 1e-6);

        // Remove non-existent quota, no change
        alters.clear();
        entityQuotaToAlterations(userEntity, quotas(
                QuotaConfigs.REQUEST_PERCENTAGE_OVERRIDE_CONFIG, null
        ), alters::add);
        alterQuotas(alters, manager);
        assertEquals(1, manager.clientQuotaData.get(userEntity).size());
        assertEquals(20000.0, manager.clientQuotaData.get(userEntity).get(QuotaConfigs.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG), 1e-6);

        // All quotas removed, we should cleanup the map
        alters.clear();
        entityQuotaToAlterations(userEntity, quotas(
                QuotaConfigs.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG, null
        ), alters::add);
        alterQuotas(alters, manager);
        assertFalse(manager.clientQuotaData.containsKey(userEntity));

        // Remove non-existent quota, again no change
        alters.clear();
        entityQuotaToAlterations(userEntity, quotas(
                QuotaConfigs.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG, null
        ), alters::add);
        alterQuotas(alters, manager);
        assertFalse(manager.clientQuotaData.containsKey(userEntity));

        // Mixed update
        alters.clear();
        Map<String, Double> quotas = new HashMap<>(4);
        quotas.put(QuotaConfigs.REQUEST_PERCENTAGE_OVERRIDE_CONFIG, 99.0);
        quotas.put(QuotaConfigs.CONTROLLER_MUTATION_RATE_OVERRIDE_CONFIG, null);
        quotas.put(QuotaConfigs.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, 10002.0);
        quotas.put(QuotaConfigs.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG, 20001.0);

        entityQuotaToAlterations(userEntity, quotas, alters::add);
        alterQuotas(alters, manager);
        assertEquals(3, manager.clientQuotaData.get(userEntity).size());
        assertEquals(20001.0, manager.clientQuotaData.get(userEntity).get(QuotaConfigs.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG), 1e-6);
        assertEquals(10002.0, manager.clientQuotaData.get(userEntity).get(QuotaConfigs.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG), 1e-6);
        assertEquals(99.0, manager.clientQuotaData.get(userEntity).get(QuotaConfigs.REQUEST_PERCENTAGE_OVERRIDE_CONFIG), 1e-6);
    }

    @Test
    public void testEntityTypes() throws Exception {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(new LogContext());
        ClientQuotaControlManager manager = new ClientQuotaControlManager(snapshotRegistry);

        Map<ClientQuotaEntity, Map<String, Double>> quotasToTest = new HashMap<>();
        quotasToTest.put(userClientEntity("user-1", "client-id-1"),
                quotas(QuotaConfigs.REQUEST_PERCENTAGE_OVERRIDE_CONFIG, 50.50));
        quotasToTest.put(userClientEntity("user-2", "client-id-1"),
                quotas(QuotaConfigs.REQUEST_PERCENTAGE_OVERRIDE_CONFIG, 51.51));
        quotasToTest.put(userClientEntity("user-3", "client-id-2"),
                quotas(QuotaConfigs.REQUEST_PERCENTAGE_OVERRIDE_CONFIG, 52.52));
        quotasToTest.put(userClientEntity(null, "client-id-1"),
                quotas(QuotaConfigs.REQUEST_PERCENTAGE_OVERRIDE_CONFIG, 53.53));
        quotasToTest.put(userClientEntity("user-1", null),
                quotas(QuotaConfigs.REQUEST_PERCENTAGE_OVERRIDE_CONFIG, 54.54));
        quotasToTest.put(userClientEntity("user-3", null),
                quotas(QuotaConfigs.REQUEST_PERCENTAGE_OVERRIDE_CONFIG, 55.55));
        quotasToTest.put(userEntity("user-1"),
                quotas(QuotaConfigs.REQUEST_PERCENTAGE_OVERRIDE_CONFIG, 56.56));
        quotasToTest.put(userEntity("user-2"),
                quotas(QuotaConfigs.REQUEST_PERCENTAGE_OVERRIDE_CONFIG, 57.57));
        quotasToTest.put(userEntity("user-3"),
                quotas(QuotaConfigs.REQUEST_PERCENTAGE_OVERRIDE_CONFIG, 58.58));
        quotasToTest.put(userEntity(null),
                quotas(QuotaConfigs.REQUEST_PERCENTAGE_OVERRIDE_CONFIG, 59.59));
        quotasToTest.put(clientEntity("client-id-2"),
                quotas(QuotaConfigs.REQUEST_PERCENTAGE_OVERRIDE_CONFIG, 60.60));

        List<ClientQuotaAlteration> alters = new ArrayList<>();
        quotasToTest.forEach((entity, quota) -> entityQuotaToAlterations(entity, quota, alters::add));
        alterQuotas(alters, manager);

        ControllerTestUtils.assertBatchIteratorContains(Arrays.asList(
            Arrays.asList(new ApiMessageAndVersion(new QuotaRecord().setEntity(Arrays.asList(
                new EntityData().setEntityType("user").setEntityName("user-1"),
                new EntityData().setEntityType("client-id").setEntityName("client-id-1"))).
                    setKey("request_percentage").setValue(50.5).setRemove(false), (short) 0)),
            Arrays.asList(new ApiMessageAndVersion(new QuotaRecord().setEntity(Arrays.asList(
                new EntityData().setEntityType("user").setEntityName("user-2"),
                new EntityData().setEntityType("client-id").setEntityName("client-id-1"))).
                    setKey("request_percentage").setValue(51.51).setRemove(false), (short) 0)),
            Arrays.asList(new ApiMessageAndVersion(new QuotaRecord().setEntity(Arrays.asList(
                new EntityData().setEntityType("user").setEntityName("user-3"),
                new EntityData().setEntityType("client-id").setEntityName("client-id-2"))).
                    setKey("request_percentage").setValue(52.52).setRemove(false), (short) 0)),
            Arrays.asList(new ApiMessageAndVersion(new QuotaRecord().setEntity(Arrays.asList(
                new EntityData().setEntityType("user").setEntityName(null),
                new EntityData().setEntityType("client-id").setEntityName("client-id-1"))).
                    setKey("request_percentage").setValue(53.53).setRemove(false), (short) 0)),
            Arrays.asList(new ApiMessageAndVersion(new QuotaRecord().setEntity(Arrays.asList(
                new EntityData().setEntityType("user").setEntityName("user-1"),
                new EntityData().setEntityType("client-id").setEntityName(null))).
                    setKey("request_percentage").setValue(54.54).setRemove(false), (short) 0)),
            Arrays.asList(new ApiMessageAndVersion(new QuotaRecord().setEntity(Arrays.asList(
                new EntityData().setEntityType("user").setEntityName("user-3"),
                new EntityData().setEntityType("client-id").setEntityName(null))).
                    setKey("request_percentage").setValue(55.55).setRemove(false), (short) 0)),
            Arrays.asList(new ApiMessageAndVersion(new QuotaRecord().setEntity(Arrays.asList(
                new EntityData().setEntityType("user").setEntityName("user-1"))).
                    setKey("request_percentage").setValue(56.56).setRemove(false), (short) 0)),
            Arrays.asList(new ApiMessageAndVersion(new QuotaRecord().setEntity(Arrays.asList(
                new EntityData().setEntityType("user").setEntityName("user-2"))).
                    setKey("request_percentage").setValue(57.57).setRemove(false), (short) 0)),
            Arrays.asList(new ApiMessageAndVersion(new QuotaRecord().setEntity(Arrays.asList(
                new EntityData().setEntityType("user").setEntityName("user-3"))).
                    setKey("request_percentage").setValue(58.58).setRemove(false), (short) 0)),
            Arrays.asList(new ApiMessageAndVersion(new QuotaRecord().setEntity(Arrays.asList(
                new EntityData().setEntityType("user").setEntityName(null))).
                    setKey("request_percentage").setValue(59.59).setRemove(false), (short) 0)),
            Arrays.asList(new ApiMessageAndVersion(new QuotaRecord().setEntity(Arrays.asList(
                new EntityData().setEntityType("client-id").setEntityName("client-id-2"))).
                    setKey("request_percentage").setValue(60.60).setRemove(false), (short) 0))),
            manager.iterator(Long.MAX_VALUE));
    }

    static void entityQuotaToAlterations(ClientQuotaEntity entity, Map<String, Double> quota,
                                          Consumer<ClientQuotaAlteration> acceptor) {
        Collection<ClientQuotaAlteration.Op> ops = quota.entrySet().stream()
                .map(quotaEntry -> new ClientQuotaAlteration.Op(quotaEntry.getKey(), quotaEntry.getValue()))
                .collect(Collectors.toList());
        acceptor.accept(new ClientQuotaAlteration(entity, ops));
    }

    static void alterQuotas(List<ClientQuotaAlteration> alterations, ClientQuotaControlManager manager) {
        ControllerResult<Map<ClientQuotaEntity, ApiError>> result = manager.alterClientQuotas(alterations);
        assertTrue(result.response().values().stream().allMatch(ApiError::isSuccess));
        result.records().forEach(apiMessageAndVersion -> manager.replay((QuotaRecord) apiMessageAndVersion.message()));
    }

    static Map<String, Double> quotas(String key, Double value) {
        return Collections.singletonMap(key, value);
    }

    static Map<String, Double> quotas(String key1, Double value1, String key2, Double value2) {
        Map<String, Double> quotas = new HashMap<>(2);
        quotas.put(key1, value1);
        quotas.put(key2, value2);
        return quotas;
    }

    static ClientQuotaEntity entity(String type, String name) {
        return new ClientQuotaEntity(Collections.singletonMap(type, name));
    }

    static ClientQuotaEntity entity(String type1, String name1, String type2, String name2) {
        Map<String, String> entries = new HashMap<>(2);
        entries.put(type1, name1);
        entries.put(type2, name2);
        return new ClientQuotaEntity(entries);
    }

    static ClientQuotaEntity userEntity(String user) {
        return new ClientQuotaEntity(Collections.singletonMap(ClientQuotaEntity.USER, user));
    }

    static ClientQuotaEntity clientEntity(String clientId) {
        return new ClientQuotaEntity(Collections.singletonMap(ClientQuotaEntity.CLIENT_ID, clientId));
    }

    static ClientQuotaEntity userClientEntity(String user, String clientId) {
        Map<String, String> entries = new HashMap<>(2);
        entries.put(ClientQuotaEntity.USER, user);
        entries.put(ClientQuotaEntity.CLIENT_ID, clientId);
        return new ClientQuotaEntity(entries);
    }
}
