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
package org.apache.kafka.server;

import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.common.message.ApiMessageType.ListenerType;
import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;
import org.apache.kafka.metadata.KRaftMetadataCache;
import org.apache.kafka.server.common.KRaftVersion;
import org.apache.kafka.server.common.MetadataVersion;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Optional;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DefaultApiVersionManagerTest {

    private final BrokerFeatures brokerFeatures = BrokerFeatures.createDefault(true);

    private final KRaftMetadataCache metadataCache = createMetadataCache();

    private static KRaftMetadataCache createMetadataCache() {
        var cache = new KRaftMetadataCache(1, () -> KRaftVersion.LATEST_PRODUCTION);
        var delta = new MetadataDelta.Builder()
            .setImage(MetadataImage.EMPTY)
            .build();
        delta.replay(new FeatureLevelRecord()
            .setName(MetadataVersion.FEATURE_NAME)
            .setFeatureLevel(MetadataVersion.latestProduction().featureLevel())
        );
        cache.setImage(delta.apply(MetadataProvenance.EMPTY));
        return cache;
    }

    @ParameterizedTest
    @EnumSource(ListenerType.class)
    public void testApiScope(ListenerType apiScope) {
        Supplier<Optional<NodeApiVersions>> nodeApiVersionsSupplier = Optional::empty;
        var versionManager = new DefaultApiVersionManager(
            apiScope,
            nodeApiVersionsSupplier,
            brokerFeatures,
            metadataCache,
            true,
            Optional.empty()
        );
        for (ApiKeys apiKey : ApiKeys.apisForListener(apiScope)) {
            for (short version : apiKey.allVersions()) {
                assertTrue(versionManager.isApiEnabled(apiKey, version));
            }
        }
    }

    @ParameterizedTest
    @EnumSource(ListenerType.class)
    public void testDisabledApis(ListenerType apiScope) {
        Supplier<Optional<NodeApiVersions>> nodeApiVersionsSupplier = Optional::empty;
        var versionManager = new DefaultApiVersionManager(
            apiScope,
            nodeApiVersionsSupplier,
            brokerFeatures,
            metadataCache,
            false,
            Optional.empty()
        );

        for (ApiKeys apiKey : ApiKeys.apisForListener(apiScope)) {
            if (apiKey.messageType.latestVersionUnstable()) {
                assertFalse(
                    versionManager.isApiEnabled(apiKey, apiKey.latestVersion()),
                    apiKey + " version " + apiKey.latestVersion() + " should be disabled."
                );
            }
        }
    }

    @Test
    public void testControllerApiIntersection() {
        short controllerMinVersion = 3;
        short controllerMaxVersion = 5;

        Supplier<Optional<NodeApiVersions>> nodeApiVersionsSupplier = () -> Optional.of(
                NodeApiVersions.create(
                    ApiKeys.CREATE_TOPICS.id,
                    controllerMinVersion,
                    controllerMaxVersion
        ));

        var versionManager = new DefaultApiVersionManager(
            ListenerType.BROKER,
            nodeApiVersionsSupplier,
            brokerFeatures,
            metadataCache,
            true,
            Optional.empty()
        );

        var apiVersionsResponse = versionManager.apiVersionResponse(0, false);
        var alterConfigVersion = apiVersionsResponse.data().apiKeys().find(ApiKeys.CREATE_TOPICS.id);
        assertNotNull(alterConfigVersion);
        assertEquals(controllerMinVersion, alterConfigVersion.minVersion());
        assertEquals(controllerMaxVersion, alterConfigVersion.maxVersion());
    }

    @Test
    public void testEnvelopeDisabledForKRaftBroker() {
        Supplier<Optional<NodeApiVersions>> nodeApiVersionsSupplier = Optional::empty;

        DefaultApiVersionManager versionManager = new DefaultApiVersionManager(
            ListenerType.BROKER,
            nodeApiVersionsSupplier,
            brokerFeatures,
            metadataCache,
            true,
            Optional.empty()
        );
        assertFalse(versionManager.isApiEnabled(ApiKeys.ENVELOPE, ApiKeys.ENVELOPE.latestVersion()));
        assertFalse(ApiKeys.apisForListener(versionManager.listenerType()).contains(ApiKeys.ENVELOPE));

        var apiVersionsResponse = versionManager.apiVersionResponse(0, false);
        var envelopeVersion = apiVersionsResponse.data().apiKeys().find(ApiKeys.ENVELOPE.id);
        assertNull(envelopeVersion);
    }
}
