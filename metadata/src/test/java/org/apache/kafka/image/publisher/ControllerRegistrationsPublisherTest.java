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

package org.apache.kafka.image.publisher;

import org.junit.jupiter.api.Timeout;


@Timeout(value = 40)
public class ControllerRegistrationsPublisherTest {
    /*
    @Test
    public void testInitialControllers() {
        ControllerRegistrationsPublisher publisher = new ControllerRegistrationsPublisher();
        assertEquals(Collections.emptyMap(), publisher.controllers());
    }

    @Test
    public void testName() {
        ControllerRegistrationsPublisher publisher = new ControllerRegistrationsPublisher();
        assertEquals("ControllerRegistrationsPublisher", publisher.name());
    }

    private static final MetadataDelta TEST_DELTA;

    private static final MetadataImage TEST_IMAGE;

    static {
        List<ApiMessageAndVersion> records = Arrays.asList(
            new ApiMessageAndVersion(
                new RegisterControllerRecord().
                    setControllerId(0).
                    setIncarnationId(Uuid.fromString("1IAc4mS9RgqR00apcA2UTQ")).
                    setZkMigrationReady(false).
                    setEndPoints(
                        new ControllerEndpointCollection(Arrays.asList(
                            new ControllerEndpoint().
                                setName("CONTROLLER").
                                setHost("example.com").
                                setPort(8080).
                                setSecurityProtocol(SecurityProtocol.SASL_PLAINTEXT.id),
                            new ControllerEndpoint().
                                setName("CONTROLLER_SSL").
                                setHost("example.com").
                                setPort(8090).
                                setSecurityProtocol(SecurityProtocol.SASL_SSL.id)
                        ).iterator())).
                    setFeatures(
                        new ControllerFeatureCollection(Arrays.asList(
                            new ControllerFeature().
                                setName(MetadataVersion.FEATURE_NAME).
                                setMinSupportedVersion((short) 1).
                                setMaxSupportedVersion((short) 13)
                        ).iterator())
                    ),
                (short) 0),
            new ApiMessageAndVersion(
                new RegisterControllerRecord().
                    setControllerId(1).
                    setIncarnationId(Uuid.fromString("yOVziEQLQO6HQK0J76EeFw")).
                    setZkMigrationReady(false).
                    setEndPoints(
                        new ControllerEndpointCollection(Arrays.asList(
                            new ControllerEndpoint().
                                setName("CONTROLLER").
                                setHost("example.com").
                                setPort(8081).
                                setSecurityProtocol(SecurityProtocol.SASL_PLAINTEXT.id),
                            new ControllerEndpoint().
                                setName("CONTROLLER_SSL").
                                setHost("example.com").
                                setPort(8091).
                                setSecurityProtocol(SecurityProtocol.SASL_SSL.id)
                        ).iterator())).
                    setFeatures(
                        new ControllerFeatureCollection(Arrays.asList(
                            new ControllerFeature().
                                setName(MetadataVersion.FEATURE_NAME).
                                setMinSupportedVersion((short) 1).
                                setMaxSupportedVersion((short) 13)
                        ).iterator())
                    ),
                (short) 0),
            new ApiMessageAndVersion(
                new RegisterControllerRecord().
                    setControllerId(2).
                    setIncarnationId(Uuid.fromString("4JXjhEtARYO85g-o3I4Ieg")).
                    setZkMigrationReady(false).
                    setEndPoints(
                        new ControllerEndpointCollection(Arrays.asList(
                            new ControllerEndpoint().
                                setName("CONTROLLER").
                                setHost("example.com").
                                setPort(8082).
                                setSecurityProtocol(SecurityProtocol.SASL_PLAINTEXT.id),
                            new ControllerEndpoint().
                                setName("CONTROLLER_SSL").
                                setHost("example.com").
                                setPort(8092).
                                setSecurityProtocol(SecurityProtocol.SASL_SSL.id)
                        ).iterator())).
                    setFeatures(
                        new ControllerFeatureCollection(Arrays.asList(
                            new ControllerFeature().
                                setName(MetadataVersion.FEATURE_NAME).
                                setMinSupportedVersion((short) 1).
                                setMaxSupportedVersion((short) 13)
                        ).iterator())
                    ),
                    (short) 0)
        );
        TEST_DELTA = new MetadataDelta.Builder().build();
        RecordTestUtils.replayAll(TEST_DELTA, records);
        TEST_IMAGE = TEST_DELTA.image();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testOnMetadataUpdate(boolean fromSnapshot) {
        ControllerRegistrationsPublisher publisher = new ControllerRegistrationsPublisher();
        if (fromSnapshot) {
            publisher.onMetadataUpdate(TEST_DELTA, TEST_IMAGE,
                new SnapshotManifest(new MetadataProvenance(100L, 10, 2000L), 100L));
        } else {
            publisher.onMetadataUpdate(TEST_DELTA, TEST_IMAGE,
                new LogDeltaManifest(new MetadataProvenance(100L, 10, 2000L),
                    new LeaderAndEpoch(OptionalInt.of(1), 200),
                    3,
                    1000L,
                    234));
        }
        assertEquals(new HashSet<>(Arrays.asList(0, 1, 2)), publisher.controllers().keySet());
    }
    */
}
