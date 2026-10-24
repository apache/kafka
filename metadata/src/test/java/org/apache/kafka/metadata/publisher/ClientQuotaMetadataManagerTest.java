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
import org.apache.kafka.image.ClientQuotaDelta;
import org.apache.kafka.image.ClientQuotaImage;
import org.apache.kafka.image.ClientQuotasDelta;
import org.apache.kafka.image.ClientQuotasImage;
import org.apache.kafka.metadata.publisher.QuotaEntity.UserClientQuotaEntity;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.function.BiConsumer;

import static org.apache.kafka.common.quota.ClientQuotaEntity.CLIENT_ID;
import static org.apache.kafka.common.quota.ClientQuotaEntity.IP;
import static org.apache.kafka.common.quota.ClientQuotaEntity.USER;
import static org.apache.kafka.server.config.QuotaConfig.IP_CONNECTION_RATE_OVERRIDE_CONFIG;
import static org.apache.kafka.server.config.QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class ClientQuotaMetadataManagerTest {
    @SuppressWarnings("unchecked")
    private final BiConsumer<UserClientQuotaEntity, OptionalDouble> userClientQuotaUpdater = mock(BiConsumer.class);
    @SuppressWarnings("unchecked")
    private final BiConsumer<Optional<InetAddress>, OptionalInt> ipQuotaUpdater = mock(BiConsumer.class);
    private final ClientQuotaMetadataManager manager = new ClientQuotaMetadataManager(
        Map.of(PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, userClientQuotaUpdater), ipQuotaUpdater);

    @Test
    public void testInvalidIpAddress() {
        assertThrows(IllegalArgumentException.class,
            () -> manager.accept(createQuotaDelta(Map.of(IP, "invalid address"), Map.of())));
    }

    @Test
    public void testUserClientQuotaUpdateAndRemoval() {
        Map<String, String> entity = Map.of(USER, "user", CLIENT_ID, "client");
        var expectedEntity = new QuotaEntity.ExplicitUserExplicitClientIdEntity("user", "client");

        manager.accept(createQuotaDelta(entity, Map.of(PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, OptionalDouble.of(123.5))));
        verify(userClientQuotaUpdater).accept(expectedEntity, OptionalDouble.of(123.5));

        manager.accept(createQuotaDelta(entity, Map.of(PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, OptionalDouble.empty())));
        verify(userClientQuotaUpdater).accept(expectedEntity, OptionalDouble.empty());
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {"192.168.1.1", "2001:db8::1"})
    public void testIpQuotaUpdateAndRemoval(String address) throws UnknownHostException {
        Map<String, String> entity = Collections.singletonMap(IP, address);
        Optional<InetAddress> inetAddress = address == null ? Optional.empty() : Optional.of(InetAddress.getByName(address));

        manager.accept(createQuotaDelta(entity, Map.of(IP_CONNECTION_RATE_OVERRIDE_CONFIG, OptionalDouble.of(123))));
        verify(ipQuotaUpdater).accept(inetAddress, OptionalInt.of(123));

        manager.accept(createQuotaDelta(entity, Map.of(IP_CONNECTION_RATE_OVERRIDE_CONFIG, OptionalDouble.empty())));
        verify(ipQuotaUpdater).accept(inetAddress, OptionalInt.empty());
    }

    private static ClientQuotasDelta createQuotaDelta(Map<String, String> entity, Map<String, OptionalDouble> quotaChanges) {
        ClientQuotaDelta quotaDelta = new ClientQuotaDelta(ClientQuotaImage.EMPTY);
        quotaDelta.changes().putAll(quotaChanges);

        ClientQuotasDelta delta = new ClientQuotasDelta(ClientQuotasImage.EMPTY);
        delta.changes().put(new ClientQuotaEntity(entity), quotaDelta);
        return delta;
    }
}
