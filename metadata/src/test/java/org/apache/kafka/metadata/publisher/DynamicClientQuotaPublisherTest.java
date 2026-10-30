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
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;
import org.apache.kafka.image.loader.SnapshotManifest;
import org.apache.kafka.metadata.publisher.DynamicClientQuotaPublisher.IpQuotaUpdater;
import org.apache.kafka.metadata.publisher.DynamicClientQuotaPublisher.UserClientQuotaUpdater;
import org.apache.kafka.server.fault.FaultHandler;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;

import static org.apache.kafka.common.quota.ClientQuotaEntity.CLIENT_ID;
import static org.apache.kafka.common.quota.ClientQuotaEntity.IP;
import static org.apache.kafka.common.quota.ClientQuotaEntity.USER;
import static org.apache.kafka.server.config.QuotaConfig.IP_CONNECTION_RATE_OVERRIDE_CONFIG;
import static org.apache.kafka.server.config.QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

public class DynamicClientQuotaPublisherTest {
    private static final SnapshotManifest MANIFEST = new SnapshotManifest(MetadataProvenance.EMPTY, 0);

    private final UserClientQuotaUpdater userClientQuotaUpdater = mock(UserClientQuotaUpdater.class);
    private final IpQuotaUpdater ipQuotaUpdater = mock(IpQuotaUpdater.class);
    private final FaultHandler faultHandler = mock(FaultHandler.class);
    private final DynamicClientQuotaPublisher publisher = new DynamicClientQuotaPublisher(
        0, faultHandler, "broker", Map.of(PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, userClientQuotaUpdater), ipQuotaUpdater);

    @Test
    public void testNoClientQuotaChanges() {
        MetadataDelta delta = new MetadataDelta.Builder().setImage(MetadataImage.EMPTY).build();
        publisher.onMetadataUpdate(delta, MetadataImage.EMPTY, MANIFEST);
        verifyNoInteractions(userClientQuotaUpdater, ipQuotaUpdater, faultHandler);
    }

    @Test
    public void testInvalidIpAddressIsReportedToFaultHandler() {
        publisher.onMetadataUpdate(createMetadataDelta(Map.of(IP, "invalid address"), Map.of()), MetadataImage.EMPTY, MANIFEST);
        verify(faultHandler).handleFault(
            eq("Uncaught exception while publishing dynamic client quota changes from MetadataDelta up to -1"),
            any(IllegalArgumentException.class));
    }

    @Test
    public void testUserClientQuotaUpdateAndRemoval() {
        List<Map<String, String>> entities = List.of(
            Collections.singletonMap(USER, null),
            Collections.singletonMap(CLIENT_ID, null),
            Map.of(USER, "user", CLIENT_ID, "client")
        );
        for (Map<String, String> entity : entities) {
            var expectedEntity = new ClientQuotaEntity(entity);

            publisher.onMetadataUpdate(createMetadataDelta(entity, Map.of(PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, OptionalDouble.of(123.5))),
                MetadataImage.EMPTY, MANIFEST);
            verify(userClientQuotaUpdater).updateQuota(expectedEntity, OptionalDouble.of(123.5));

            publisher.onMetadataUpdate(createMetadataDelta(entity, Map.of(PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, OptionalDouble.empty())),
                MetadataImage.EMPTY, MANIFEST);
            verify(userClientQuotaUpdater).updateQuota(expectedEntity, OptionalDouble.empty());
        }
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {"192.168.1.1", "2001:db8::1"})
    public void testIpQuotaUpdateAndRemoval(String address) throws UnknownHostException {
        Map<String, String> entity = Collections.singletonMap(IP, address);
        Optional<InetAddress> inetAddress = address == null ? Optional.empty() : Optional.of(InetAddress.getByName(address));

        publisher.onMetadataUpdate(createMetadataDelta(entity, Map.of(IP_CONNECTION_RATE_OVERRIDE_CONFIG, OptionalDouble.of(123))),
            MetadataImage.EMPTY, MANIFEST);
        verify(ipQuotaUpdater).updateQuota(inetAddress, OptionalInt.of(123));

        publisher.onMetadataUpdate(createMetadataDelta(entity, Map.of(IP_CONNECTION_RATE_OVERRIDE_CONFIG, OptionalDouble.empty())),
            MetadataImage.EMPTY, MANIFEST);
        verify(ipQuotaUpdater).updateQuota(inetAddress, OptionalInt.empty());
    }

    private static MetadataDelta createMetadataDelta(Map<String, String> entity, Map<String, OptionalDouble> quotaChanges) {
        ClientQuotaDelta quotaDelta = new ClientQuotaDelta(ClientQuotaImage.EMPTY);
        quotaDelta.changes().putAll(quotaChanges);

        MetadataDelta delta = new MetadataDelta.Builder().setImage(MetadataImage.EMPTY).build();
        delta.getOrCreateClientQuotasDelta().changes().put(new ClientQuotaEntity(entity), quotaDelta);
        return delta;
    }
}
