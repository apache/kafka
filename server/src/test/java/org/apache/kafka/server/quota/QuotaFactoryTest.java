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

import org.apache.kafka.common.metrics.Quota;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.server.quota.ClientQuotaEntity.ConfigEntity;
import org.apache.kafka.server.quota.QuotaFactory.QuotaManagers;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;

import static org.apache.kafka.common.quota.ClientQuotaEntity.CLIENT_ID;
import static org.apache.kafka.common.quota.ClientQuotaEntity.USER;
import static org.apache.kafka.server.config.QuotaConfig.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG;
import static org.apache.kafka.server.config.QuotaConfig.CONTROLLER_MUTATION_RATE_OVERRIDE_CONFIG;
import static org.apache.kafka.server.config.QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG;
import static org.apache.kafka.server.config.QuotaConfig.REQUEST_PERCENTAGE_OVERRIDE_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class QuotaFactoryTest {
    @Test
    public void testUserClientQuotaUpdaterConvertsEntityAndQuota() {
        assertQuotaConversion(
            Map.of(USER, "user"),
            Optional.of(new ClientQuotaManager.UserEntity("user")), Optional.empty()
        );
        assertQuotaConversion(
            Collections.singletonMap(USER, null),
            Optional.of(ClientQuotaManager.DEFAULT_USER_ENTITY), Optional.empty()
        );
        assertQuotaConversion(
            Map.of(CLIENT_ID, "client"),
            Optional.empty(), Optional.of(new ClientQuotaManager.ClientIdEntity("client"))
        );
        assertQuotaConversion(
            Collections.singletonMap(CLIENT_ID, null),
            Optional.empty(), Optional.of(ClientQuotaManager.DEFAULT_USER_CLIENT_ID)
        );
        assertQuotaConversion(
            Map.of(USER, "user", CLIENT_ID, "client"),
            Optional.of(new ClientQuotaManager.UserEntity("user")), Optional.of(new ClientQuotaManager.ClientIdEntity("client"))
        );

        Map<String, String> explicitUserDefaultClient = new HashMap<>();
        explicitUserDefaultClient.put(USER, "user");
        explicitUserDefaultClient.put(CLIENT_ID, null);
        assertQuotaConversion(
            explicitUserDefaultClient,
            Optional.of(new ClientQuotaManager.UserEntity("user")), Optional.of(ClientQuotaManager.DEFAULT_USER_CLIENT_ID)
        );

        Map<String, String> defaultUserExplicitClient = new HashMap<>();
        defaultUserExplicitClient.put(USER, null);
        defaultUserExplicitClient.put(CLIENT_ID, "client");
        assertQuotaConversion(
            defaultUserExplicitClient,
            Optional.of(ClientQuotaManager.DEFAULT_USER_ENTITY), Optional.of(new ClientQuotaManager.ClientIdEntity("client"))
        );

        Map<String, String> defaultUserDefaultClient = new HashMap<>();
        defaultUserDefaultClient.put(USER, null);
        defaultUserDefaultClient.put(CLIENT_ID, null);
        assertQuotaConversion(
            defaultUserDefaultClient,
            Optional.of(ClientQuotaManager.DEFAULT_USER_ENTITY), Optional.of(ClientQuotaManager.DEFAULT_USER_CLIENT_ID)
        );
    }

    @Test
    public void testUserClientQuotaUpdatersDispatchToManagers() {
        QuotaManagers managers = createQuotaManagers(mock(ClientQuotaManager.class));
        Map<String, ClientQuotaManager> expectedManagers = Map.of(
            CONSUMER_BYTE_RATE_OVERRIDE_CONFIG, managers.fetch(),
            PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, managers.produce(),
            REQUEST_PERCENTAGE_OVERRIDE_CONFIG, managers.request(),
            CONTROLLER_MUTATION_RATE_OVERRIDE_CONFIG, managers.controllerMutation()
        );

        var userClientQuotaUpdaters = managers.userClientQuotaUpdaters();
        assertEquals(expectedManagers.keySet(), userClientQuotaUpdaters.keySet());

        var entity = new ClientQuotaEntity(Map.of(USER, "user", CLIENT_ID, "client"));
        var quota = OptionalDouble.of(123.5);
        Optional<ConfigEntity> expectedUser = Optional.of(new ClientQuotaManager.UserEntity("user"));
        Optional<ConfigEntity> expectedClient = Optional.of(new ClientQuotaManager.ClientIdEntity("client"));
        expectedManagers.forEach((key, manager) -> {
            userClientQuotaUpdaters.get(key).accept(entity, quota);
            verify(manager).updateQuota(expectedUser, expectedClient, Optional.of(Quota.upperBound(123.5)));
        });
    }

    private static QuotaManagers createQuotaManagers(ClientQuotaManager produce) {
        return new QuotaManagers(
            mock(ClientQuotaManager.class), produce, mock(ClientRequestQuotaManager.class),
            mock(ControllerMutationQuotaManager.class), null, null, null, null, Optional.empty()
        );
    }

    private static void assertQuotaConversion(
        Map<String, String> entries,
        Optional<ConfigEntity> expectedUser,
        Optional<ConfigEntity> expectedClient
    ) {
        ClientQuotaManager manager = mock(ClientQuotaManager.class);
        var userClientQuotaUpdater = createQuotaManagers(manager).userClientQuotaUpdaters().get(PRODUCER_BYTE_RATE_OVERRIDE_CONFIG);
        var entity = new ClientQuotaEntity(entries);

        // Verify quota updates.
        userClientQuotaUpdater.accept(entity, OptionalDouble.of(123.5));
        verify(manager).updateQuota(expectedUser, expectedClient, Optional.of(Quota.upperBound(123.5)));

        // Verify quota removal.
        userClientQuotaUpdater.accept(entity, OptionalDouble.empty());
        verify(manager).updateQuota(expectedUser, expectedClient, Optional.empty());
    }
}
