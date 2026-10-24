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

package org.apache.kafka.metadata.publisher;

import org.apache.kafka.common.quota.ClientQuotaEntity;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.kafka.common.quota.ClientQuotaEntity.CLIENT_ID;
import static org.apache.kafka.common.quota.ClientQuotaEntity.IP;
import static org.apache.kafka.common.quota.ClientQuotaEntity.USER;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class QuotaEntityTest {
    @Test
    public void testFromClientQuotaEntity() {
        assertQuotaEntity(Map.of(IP, "192.168.1.1"), new QuotaEntity.IpEntity("192.168.1.1"));
        assertQuotaEntity(Collections.singletonMap(IP, null), new QuotaEntity.DefaultIpEntity());

        assertQuotaEntity(Map.of(USER, "user"), new QuotaEntity.UserEntity("user"));
        assertQuotaEntity(Collections.singletonMap(USER, null), new QuotaEntity.DefaultUserEntity());

        assertQuotaEntity(Map.of(CLIENT_ID, "client"), new QuotaEntity.ClientIdEntity("client"));
        assertQuotaEntity(Collections.singletonMap(CLIENT_ID, null), new QuotaEntity.DefaultClientIdEntity());

        Map<String, String> explicitUserExplicitClient = Map.of(USER, "user", CLIENT_ID, "client");
        assertQuotaEntity(explicitUserExplicitClient, new QuotaEntity.ExplicitUserExplicitClientIdEntity("user", "client"));

        Map<String, String> explicitUserDefaultClient = new HashMap<>();
        explicitUserDefaultClient.put(USER, "user");
        explicitUserDefaultClient.put(CLIENT_ID, null);
        assertQuotaEntity(explicitUserDefaultClient, new QuotaEntity.ExplicitUserDefaultClientIdEntity("user"));

        Map<String, String> defaultUserExplicitClient = new HashMap<>();
        defaultUserExplicitClient.put(USER, null);
        defaultUserExplicitClient.put(CLIENT_ID, "client");
        assertQuotaEntity(defaultUserExplicitClient, new QuotaEntity.DefaultUserExplicitClientIdEntity("client"));

        Map<String, String> defaultUserDefaultClient = new HashMap<>();
        defaultUserDefaultClient.put(USER, null);
        defaultUserDefaultClient.put(CLIENT_ID, null);
        assertQuotaEntity(defaultUserDefaultClient, new QuotaEntity.DefaultUserDefaultClientIdEntity());
    }

    private static void assertQuotaEntity(Map<String, String> entries, QuotaEntity expectedEntity) {
        assertEquals(Optional.of(expectedEntity), QuotaEntity.fromClientQuotaEntity(new ClientQuotaEntity(entries)));
    }
}
