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

package org.apache.kafka.image.node;

import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;


@Timeout(value = 40)
public class ClientQuotasImageNodeTest {
    @Test
    public void testEscapeEmptyString() {
        assertEquals("", ClientQuotasImageNode.escape(""));
    }

    @Test
    public void testEscapeNormalString() {
        assertEquals("abracadabra", ClientQuotasImageNode.escape("abracadabra"));
    }

    @Test
    public void testEscapeBackslashes() {
        assertEquals("\\\\foo\\\\bar", ClientQuotasImageNode.escape("\\foo\\bar"));
    }

    @Test
    public void testEscapeParentheses() {
        assertEquals("\\(bob's name\\)", ClientQuotasImageNode.escape("(bob's name)"));
    }

    private void entityToStringRoundTrip(ClientQuotaEntity entity, String expected) {
        String entityString = ClientQuotasImageNode.clientQuotaEntityToString(entity);
        assertEquals(expected, entityString);
        ClientQuotaEntity entity2 = ClientQuotasImageNode.decodeEntity(entityString);
        assertEquals(entity, entity2);
    }

    @Test
    public void clientIdEntityRoundTrip() {
        entityToStringRoundTrip(new ClientQuotaEntity(singletonMap("client-id", "foo")),
            "clientId(foo)");
    }

    @Test
    public void defaultClientIdEntityRoundTrip() {
        entityToStringRoundTrip(new ClientQuotaEntity(singletonMap("client-id", "")),
            "clientId()");
    }

    @Test
    public void userEntityRoundTrip() {
        entityToStringRoundTrip(new ClientQuotaEntity(singletonMap("user", "my-user-name")),
            "user(my-user-name)");
    }

    @Test
    public void defaultUserEntityRoundTrip() {
        entityToStringRoundTrip(new ClientQuotaEntity(singletonMap("user", "")),
            "user()");
    }

    @Test
    public void clientIdAndUserEntityRoundTrip() {
        Map<String, String> entityMap = new HashMap<>();
        entityMap.put("user", "bob");
        entityMap.put("client-id", "reports12345");
        entityToStringRoundTrip(new ClientQuotaEntity(entityMap),
            "clientId(reports12345)_user(bob)");
    }

    @Test
    public void ipEntityRoundTrip() {
        entityToStringRoundTrip(new ClientQuotaEntity(singletonMap("ip", "127.0.0.1")),
            "ip(127.0.0.1)");
    }

    @Test
    public void defaultIpEntityRoundTrip() {
        entityToStringRoundTrip(new ClientQuotaEntity(singletonMap("ip", "")),
            "ip()");
    }

    @Test
    public void testUserEntityWithBackslashesInNameRoundTrip() {
        entityToStringRoundTrip(new ClientQuotaEntity(singletonMap("user", "foo\\bar")),
            "user(foo\\\\bar)");
    }

    @Test
    public void testClientIdEntityWithParentheses() {
        entityToStringRoundTrip(new ClientQuotaEntity(singletonMap("client-id", "(this )one)")),
                "clientId(\\(this \\)one\\))");
    }

    @Test
    public void testErrorOnInvalidEmptyEntityName() {
        assertEquals("Invalid empty entity",
            assertThrows(RuntimeException.class, () -> ClientQuotasImageNode.
                clientQuotaEntityToString(new ClientQuotaEntity(emptyMap()))).
                    getMessage());
    }

    @Test
    public void testErrorOnInvalidEntityType() {
        assertEquals("Invalid entity type foobar",
            assertThrows(RuntimeException.class, () -> ClientQuotasImageNode.
                clientQuotaEntityToString(new ClientQuotaEntity(singletonMap("foobar", "baz")))).
                    getMessage());
    }
}
