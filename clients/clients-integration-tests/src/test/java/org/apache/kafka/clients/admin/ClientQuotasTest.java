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

import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.quota.ClientQuotaFilter;
import org.apache.kafka.common.quota.ClientQuotaFilterComponent;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.test.TestUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ClusterTestDefaults(types = {Type.KRAFT})
public class ClientQuotasTest {

    private Map<ClientQuotaEntity, Map<String, Double>> alterThenDescribe(
        Admin admin,
        ClientQuotaEntity entity,
        List<ClientQuotaAlteration.Op> quotas,
        ClientQuotaFilter filter,
        int expectCount
    ) throws Exception {
        AlterClientQuotasResult alterResult = admin.alterClientQuotas(List.of(new ClientQuotaAlteration(entity, quotas)));
        alterResult.all().get();

        TestUtils.waitForCondition(() -> {
            Map<ClientQuotaEntity, Map<String, Double>> results = admin.describeClientQuotas(filter).entities().get();
            return results.getOrDefault(entity, Map.of()).size() == expectCount;
        }, "Broker never saw new client quotas");

        return admin.describeClientQuotas(filter).entities().get();
    }

    private void setConsumerByteRate(Admin admin, ClientQuotaEntity entity, Long value) throws Exception {
        admin.alterClientQuotas(List.of(
            new ClientQuotaAlteration(entity, List.of(
                new ClientQuotaAlteration.Op("consumer_byte_rate", value.doubleValue())))
        )).all().get();
    }
    private Map<ClientQuotaEntity, Long> getConsumerByteRates(Admin admin) throws Exception {
        return admin.describeClientQuotas(ClientQuotaFilter.contains(List.of()))
            .entities().get()
            .entrySet().stream()
            .filter(entry -> entry.getValue().containsKey("consumer_byte_rate"))
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> entry.getValue().get("consumer_byte_rate").longValue()
            ));
    }
    @ClusterTest
    public void testClientQuotas(ClusterInstance cluster) throws Exception {
        try (Admin admin = cluster.admin()) {
            ClientQuotaEntity entity = new ClientQuotaEntity(Map.of("user", "testkit"));
            ClientQuotaFilter filter = ClientQuotaFilter.containsOnly(
                List.of(ClientQuotaFilterComponent.ofEntity("user", "testkit")));

            Map<ClientQuotaEntity, Map<String, Double>> describeResult = alterThenDescribe(admin, entity,
                List.of(new ClientQuotaAlteration.Op("request_percentage", 0.99)), filter, 1);
            assertEquals(0.99, describeResult.get(entity).get("request_percentage"), 1e-6);

            describeResult = alterThenDescribe(admin, entity, List.of(
                new ClientQuotaAlteration.Op("request_percentage", 0.97),
                new ClientQuotaAlteration.Op("producer_byte_rate", 10000.0),
                new ClientQuotaAlteration.Op("consumer_byte_rate", 10001.0)
            ), filter, 3);
            assertEquals(0.97, describeResult.get(entity).get("request_percentage"), 1e-6);
            assertEquals(10000.0, describeResult.get(entity).get("producer_byte_rate"), 1e-6);
            assertEquals(10001.0, describeResult.get(entity).get("consumer_byte_rate"), 1e-6);

            describeResult = alterThenDescribe(admin, entity, List.of(
                new ClientQuotaAlteration.Op("request_percentage", 0.95),
                new ClientQuotaAlteration.Op("producer_byte_rate", null),
                new ClientQuotaAlteration.Op("consumer_byte_rate", null)
            ), filter, 1);
            assertEquals(0.95, describeResult.get(entity).get("request_percentage"), 1e-6);

            alterThenDescribe(admin, entity, List.of(
                new ClientQuotaAlteration.Op("request_percentage", null)), filter, 0);

            describeResult = alterThenDescribe(admin, entity,
                List.of(new ClientQuotaAlteration.Op("producer_byte_rate", 9999.0)), filter, 1);
            assertEquals(9999.0, describeResult.get(entity).get("producer_byte_rate"), 1e-6);

            ClientQuotaEntity entity2 = new ClientQuotaEntity(Map.of("user", "testkit", "client-id", "some-client"));
            filter = ClientQuotaFilter.containsOnly(
                List.of(
                    ClientQuotaFilterComponent.ofEntity("user", "testkit"),
                    ClientQuotaFilterComponent.ofEntity("client-id", "some-client")
                ));
            describeResult = alterThenDescribe(admin, entity2,
                List.of(new ClientQuotaAlteration.Op("producer_byte_rate", 9998.0)), filter, 1);
            assertEquals(9998.0, describeResult.get(entity2).get("producer_byte_rate"), 1e-6);

            final ClientQuotaFilter finalFilter = ClientQuotaFilter.contains(
                List.of(ClientQuotaFilterComponent.ofEntity("user", "testkit")));

            TestUtils.waitForCondition(() -> {
                Map<ClientQuotaEntity, Map<String, Double>> results = admin.describeClientQuotas(finalFilter).entities().get();
                if (results.size() != 2) {
                    return false;
                }
                assertEquals(9999.0, results.get(entity).get("producer_byte_rate"), 1e-6);
                assertEquals(9998.0, results.get(entity2).get("producer_byte_rate"), 1e-6);
                return true;
            }, "Broker did not see two client quotas");
        }
    }

    @ClusterTest
    public void testDefaultClientQuotas(ClusterInstance cluster) throws Exception {
        try (Admin admin = cluster.admin()) {
            ClientQuotaEntity defaultUser = new ClientQuotaEntity(Collections.singletonMap("user", null));
            ClientQuotaEntity bobUser = new ClientQuotaEntity(Map.of("user", "bob"));

            TestUtils.waitForCondition(
                () -> getConsumerByteRates(admin).isEmpty(),
                "Initial consumer byte rates should be empty");

            setConsumerByteRate(admin, defaultUser, 100L);
            TestUtils.waitForCondition(() -> {
                Map<ClientQuotaEntity, Long> rates = getConsumerByteRates(admin);
                return rates.size() == 1 &&
                    rates.get(defaultUser) == 100L;
            }, "Default user rate should be 100");

            setConsumerByteRate(admin, bobUser, 1000L);
            TestUtils.waitForCondition(() -> {
                Map<ClientQuotaEntity, Long> rates = getConsumerByteRates(admin);
                return rates.size() == 2 &&
                    rates.get(defaultUser) == 100L &&
                    rates.get(bobUser) == 1000L;
            }, "Should have both default and bob user rates");
        }
    }
}
