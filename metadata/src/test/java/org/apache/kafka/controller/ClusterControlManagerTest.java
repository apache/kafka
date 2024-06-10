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

import org.apache.kafka.common.DirectoryId;
import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InconsistentClusterIdException;
import org.apache.kafka.common.errors.InvalidRegistrationException;
import org.apache.kafka.common.errors.StaleBrokerEpochException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.BrokerRegistrationRequestData;
import org.apache.kafka.common.message.ControllerRegistrationRequestData;
import org.apache.kafka.common.metadata.BrokerRegistrationChangeRecord;
import org.apache.kafka.common.metadata.FenceBrokerRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord.BrokerEndpoint;
import org.apache.kafka.common.metadata.RegisterBrokerRecord.BrokerEndpointCollection;
import org.apache.kafka.common.metadata.UnfenceBrokerRecord;
import org.apache.kafka.common.metadata.UnregisterBrokerRecord;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.metadata.BrokerRegistration;
import org.apache.kafka.metadata.BrokerRegistrationFencingChange;
import org.apache.kafka.metadata.BrokerRegistrationInControlledShutdownChange;
import org.apache.kafka.metadata.BrokerRegistrationReply;
import org.apache.kafka.metadata.FinalizedControllerFeatures;
import org.apache.kafka.metadata.RecordTestUtils;
import org.apache.kafka.metadata.VersionRange;
import org.apache.kafka.metadata.placement.ClusterDescriber;
import org.apache.kafka.metadata.placement.PartitionAssignment;
import org.apache.kafka.metadata.placement.PlacementSpec;
import org.apache.kafka.metadata.placement.UsableBroker;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.test.TestUtils;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.apache.kafka.server.common.MetadataVersion.IBP_3_3_IV2;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Timeout(value = 40)
public class ClusterControlManagerTest {
    @ParameterizedTest
    @EnumSource(value = MetadataVersion.class, names = {"IBP_3_0_IV1", "IBP_3_3_IV2"})
    public void testReplay(MetadataVersion metadataVersion) {
        MockTime time = new MockTime(0, 0, 0);

        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(new LogContext());
        FeatureControlManager featureControl = new FeatureControlManager.Builder().
            setSnapshotRegistry(snapshotRegistry).
            setQuorumFeatures(new QuorumFeatures(0,
                QuorumFeatures.defaultFeatureMap(true),
                Collections.singletonList(0))).
            build();
        ClusterControlManager clusterControl = new ClusterControlManager.Builder().
            setTime(time).
            setSnapshotRegistry(snapshotRegistry).
            setSessionTimeoutNs(1000).
            setFeatureControlManager(featureControl).
            setBrokerUncleanShutdownHandler((brokerId, records) -> { }).
            build();
        clusterControl.activate();
        assertFalse(clusterControl.isUnfenced(0));

        RegisterBrokerRecord brokerRecord = new RegisterBrokerRecord().setBrokerEpoch(100).setBrokerId(1);
        brokerRecord.endPoints().add(new BrokerEndpoint().
            setSecurityProtocol(SecurityProtocol.PLAINTEXT.id).
            setPort((short) 9092).
            setName("PLAINTEXT").
            setHost("example.com"));
        clusterControl.replay(brokerRecord, 100L);
        clusterControl.checkBrokerEpoch(1, 100);
        assertThrows(StaleBrokerEpochException.class,
            () -> clusterControl.checkBrokerEpoch(1, 101));
        assertThrows(StaleBrokerEpochException.class,
            () -> clusterControl.checkBrokerEpoch(2, 100));
        assertFalse(clusterControl.isUnfenced(0));
        assertFalse(clusterControl.isUnfenced(1));

        if (metadataVersion.isLessThan(IBP_3_3_IV2)) {
            UnfenceBrokerRecord unfenceBrokerRecord =
                    new UnfenceBrokerRecord().setId(1).setEpoch(100);
            clusterControl.replay(unfenceBrokerRecord);
        } else {
            BrokerRegistrationChangeRecord changeRecord =
                    new BrokerRegistrationChangeRecord().setBrokerId(1).setBrokerEpoch(100).setFenced(BrokerRegistrationFencingChange.UNFENCE.value());
            clusterControl.replay(changeRecord);
        }
        assertFalse(clusterControl.isUnfenced(0));
        assertTrue(clusterControl.isUnfenced(1));

        if (metadataVersion.isLessThan(IBP_3_3_IV2)) {
            FenceBrokerRecord fenceBrokerRecord =
                    new FenceBrokerRecord().setId(1).setEpoch(100);
            clusterControl.replay(fenceBrokerRecord);
        } else {
            BrokerRegistrationChangeRecord changeRecord =
                    new BrokerRegistrationChangeRecord().setBrokerId(1).setBrokerEpoch(100).setFenced(BrokerRegistrationFencingChange.FENCE.value());
            clusterControl.replay(changeRecord);
        }
        assertFalse(clusterControl.isUnfenced(0));
        assertFalse(clusterControl.isUnfenced(1));
    }

    @Test
    public void testReplayRegisterBrokerRecord() {
        MockTime time = new MockTime(0, 0, 0);

        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(new LogContext());
        FeatureControlManager featureControl = new FeatureControlManager.Builder().
            setSnapshotRegistry(snapshotRegistry).
            setQuorumFeatures(new QuorumFeatures(0,
                QuorumFeatures.defaultFeatureMap(true),
                Collections.singletonList(0))).
            build();
        ClusterControlManager clusterControl = new ClusterControlManager.Builder().
            setClusterId("fPZv1VBsRFmnlRvmGcOW9w").
            setTime(time).
            setSnapshotRegistry(snapshotRegistry).
            setSessionTimeoutNs(1000).
            setFeatureControlManager(featureControl).
            setBrokerUncleanShutdownHandler((brokerId, records) -> { }).
            build();

        assertFalse(clusterControl.isUnfenced(0));
        assertFalse(clusterControl.inControlledShutdown(0));

        RegisterBrokerRecord brokerRecord = new RegisterBrokerRecord().
            setBrokerEpoch(100).
            setBrokerId(0).
            setRack(null).
            setFenced(true).
            setInControlledShutdown(true);
        brokerRecord.endPoints().add(new BrokerEndpoint().
            setSecurityProtocol(SecurityProtocol.PLAINTEXT.id).
            setPort((short) 9092).
            setName("PLAINTEXT").
            setHost("example.com"));
        clusterControl.replay(brokerRecord, 100L);

        assertFalse(clusterControl.isUnfenced(0));
        assertTrue(clusterControl.inControlledShutdown(0));

        brokerRecord.setInControlledShutdown(false);
        clusterControl.replay(brokerRecord, 100L);

        assertFalse(clusterControl.isUnfenced(0));
        assertFalse(clusterControl.inControlledShutdown(0));
        assertEquals(100L, clusterControl.registerBrokerRecordOffset(brokerRecord.brokerId()).getAsLong());

        brokerRecord.setFenced(false);
        clusterControl.replay(brokerRecord, 100L);

        assertTrue(clusterControl.isUnfenced(0));
        assertFalse(clusterControl.inControlledShutdown(0));
    }

    @Test
    public void testReplayBrokerRegistrationChangeRecord() {
        MockTime time = new MockTime(0, 0, 0);

        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(new LogContext());
        FeatureControlManager featureControl = new FeatureControlManager.Builder().
            setSnapshotRegistry(snapshotRegistry).
            setQuorumFeatures(new QuorumFeatures(0,
                QuorumFeatures.defaultFeatureMap(true),
                Collections.singletonList(0))).
            build();
        ClusterControlManager clusterControl = new ClusterControlManager.Builder().
            setClusterId("fPZv1VBsRFmnlRvmGcOW9w").
            setTime(time).
            setSnapshotRegistry(snapshotRegistry).
            setSessionTimeoutNs(1000).
            setFeatureControlManager(featureControl).
            setBrokerUncleanShutdownHandler((brokerId, records) -> { }).
            build();

        assertFalse(clusterControl.isUnfenced(0));
        assertFalse(clusterControl.inControlledShutdown(0));

        RegisterBrokerRecord brokerRecord = new RegisterBrokerRecord().
            setBrokerEpoch(100).
            setBrokerId(0).
            setRack(null).
            setFenced(false);
        brokerRecord.endPoints().add(new BrokerEndpoint().
            setSecurityProtocol(SecurityProtocol.PLAINTEXT.id).
            setPort((short) 9092).
            setName("PLAINTEXT").
            setHost("example.com"));
        clusterControl.replay(brokerRecord, 100L);

        assertTrue(clusterControl.isUnfenced(0));
        assertFalse(clusterControl.inControlledShutdown(0));

        BrokerRegistrationChangeRecord registrationChangeRecord = new BrokerRegistrationChangeRecord()
            .setBrokerId(0)
            .setBrokerEpoch(100)
            .setInControlledShutdown(BrokerRegistrationInControlledShutdownChange.IN_CONTROLLED_SHUTDOWN.value());
        clusterControl.replay(registrationChangeRecord);

        assertTrue(clusterControl.isUnfenced(0));
        assertTrue(clusterControl.inControlledShutdown(0));

        registrationChangeRecord = new BrokerRegistrationChangeRecord()
            .setBrokerId(0)
            .setBrokerEpoch(100)
            .setFenced(BrokerRegistrationFencingChange.UNFENCE.value());
        clusterControl.replay(registrationChangeRecord);

        assertTrue(clusterControl.isUnfenced(0));
        assertTrue(clusterControl.inControlledShutdown(0));
    }

    @Test
    public void testRegistrationWithIncorrectClusterId() {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(new LogContext());
        FeatureControlManager featureControl = new FeatureControlManager.Builder().
            setSnapshotRegistry(snapshotRegistry).
            setQuorumFeatures(new QuorumFeatures(0,
                QuorumFeatures.defaultFeatureMap(true),
                Collections.singletonList(0))).
            build();
        ClusterControlManager clusterControl = new ClusterControlManager.Builder().
            setClusterId("fPZv1VBsRFmnlRvmGcOW9w").
            setTime(new MockTime(0, 0, 0)).
            setSnapshotRegistry(snapshotRegistry).
            setSessionTimeoutNs(1000).
            setFeatureControlManager(featureControl).
            setBrokerUncleanShutdownHandler((brokerId, records) -> { }).
            build();
        clusterControl.activate();
        assertThrows(InconsistentClusterIdException.class, () ->
            clusterControl.registerBroker(new BrokerRegistrationRequestData().
                    setClusterId("WIjw3grwRZmR2uOpdpVXbg").
                    setBrokerId(0).
                    setRack(null).
                    setIncarnationId(Uuid.fromString("0H4fUu1xQEKXFYwB1aBjhg")),
                123L,
                new FinalizedControllerFeatures(Collections.emptyMap(), 456L),
                (short) 1));
    }

    private static Stream<Arguments> metadataVersions() {
        return Stream.of(
                MetadataVersion.IBP_3_3_IV2,
                MetadataVersion.IBP_3_3_IV3,
                MetadataVersion.IBP_3_7_IV2, // introduces directory assignment
                MetadataVersion.latestTesting()
            ).map(Arguments::of);
    }

    @ParameterizedTest
    @MethodSource("metadataVersions")
    public void testRegisterBrokerRecordVersion(MetadataVersion metadataVersion) {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(new LogContext());
        FeatureControlManager featureControl = new FeatureControlManager.Builder().
            setSnapshotRegistry(snapshotRegistry).
            setQuorumFeatures(new QuorumFeatures(0,
                QuorumFeatures.defaultFeatureMap(true),
                Collections.singletonList(0))).
            setMetadataVersion(metadataVersion).
            build();
        ClusterControlManager clusterControl = new ClusterControlManager.Builder().
            setClusterId("fPZv1VBsRFmnlRvmGcOW9w").
            setTime(new MockTime(0, 0, 0)).
            setSnapshotRegistry(snapshotRegistry).
            setSessionTimeoutNs(1000).
            setFeatureControlManager(featureControl).
            setBrokerUncleanShutdownHandler((brokerId, records) -> { }).
            build();
        clusterControl.activate();

        List<Uuid> logDirs = metadataVersion.isDirectoryAssignmentSupported() ? asList(
                Uuid.fromString("63k9SN1nQOS0dFHSCIMA0A"),
                Uuid.fromString("Vm1MjsOCR1OjDDydOsDbzg")
        ) : Collections.emptyList();
        ControllerResult<BrokerRegistrationReply> result = clusterControl.registerBroker(
            new BrokerRegistrationRequestData().
                setClusterId("fPZv1VBsRFmnlRvmGcOW9w").
                setBrokerId(0).
                setLogDirs(logDirs).
                setRack(null).
                setIncarnationId(Uuid.fromString("0H4fUu1xQEKXFYwB1aBjhg")),
            123L,
            new FinalizedControllerFeatures(Collections.emptyMap(), 456L),
            (short) 1);

        short expectedVersion = metadataVersion.registerBrokerRecordVersion();

        assertEquals(
            asList(new ApiMessageAndVersion(new RegisterBrokerRecord().
                setBrokerEpoch(123L).
                setBrokerId(0).
                setRack(null).
                setIncarnationId(Uuid.fromString("0H4fUu1xQEKXFYwB1aBjhg")).
                setFenced(true).
                setLogDirs(logDirs).
                setFeatures(new RegisterBrokerRecord.BrokerFeatureCollection(asList(
                    new RegisterBrokerRecord.BrokerFeature().
                        setName(MetadataVersion.FEATURE_NAME).
                        setMinSupportedVersion((short) 1).
                        setMaxSupportedVersion((short) 1)).iterator())).
                setInControlledShutdown(false), expectedVersion)),
            result.records());
    }

    @Test
    public void testUnregister() {
        RegisterBrokerRecord brokerRecord = new RegisterBrokerRecord().
            setBrokerId(1).
            setBrokerEpoch(100).
            setIncarnationId(Uuid.fromString("fPZv1VBsRFmnlRvmGcOW9w")).
            setRack("arack");
        brokerRecord.endPoints().add(new BrokerEndpoint().
            setSecurityProtocol(SecurityProtocol.PLAINTEXT.id).
            setPort((short) 9092).
            setName("PLAINTEXT").
            setHost("example.com"));
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(new LogContext());
        FeatureControlManager featureControl = new FeatureControlManager.Builder().
            setSnapshotRegistry(snapshotRegistry).
            setQuorumFeatures(new QuorumFeatures(0,
                QuorumFeatures.defaultFeatureMap(true),
                Collections.singletonList(0))).
            build();
        ClusterControlManager clusterControl = new ClusterControlManager.Builder().
            setTime(new MockTime(0, 0, 0)).
            setSnapshotRegistry(snapshotRegistry).
            setSessionTimeoutNs(1000).
            setFeatureControlManager(featureControl).
            setBrokerUncleanShutdownHandler((brokerId, records) -> { }).
            build();
        clusterControl.activate();
        clusterControl.replay(brokerRecord, 100L);
        assertEquals(new BrokerRegistration.Builder().
            setId(1).
            setEpoch(100).
            setIncarnationId(Uuid.fromString("fPZv1VBsRFmnlRvmGcOW9w")).
            setListeners(Collections.singletonMap("PLAINTEXT",
                new Endpoint("PLAINTEXT", SecurityProtocol.PLAINTEXT, "example.com", 9092))).
            setRack(Optional.of("arack")).
            setFenced(true).
            setInControlledShutdown(false).build(),
            clusterControl.brokerRegistrations().get(1));
        assertEquals(100L, clusterControl.registerBrokerRecordOffset(brokerRecord.brokerId()).getAsLong());
        UnregisterBrokerRecord unregisterRecord = new UnregisterBrokerRecord().
            setBrokerId(1).
            setBrokerEpoch(100);
        clusterControl.replay(unregisterRecord);
        assertFalse(clusterControl.brokerRegistrations().containsKey(1));
        assertFalse(clusterControl.registerBrokerRecordOffset(brokerRecord.brokerId()).isPresent());
    }

    @ParameterizedTest
    @ValueSource(ints = {3, 10})
    public void testPlaceReplicas(int numUsableBrokers) {
        MockTime time = new MockTime(0, 0, 0);
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(new LogContext());
        FeatureControlManager featureControl = new FeatureControlManager.Builder().
            setSnapshotRegistry(snapshotRegistry).
            setQuorumFeatures(new QuorumFeatures(0,
                QuorumFeatures.defaultFeatureMap(true),
                Collections.singletonList(0))).
            build();
        ClusterControlManager clusterControl = new ClusterControlManager.Builder().
            setTime(time).
            setSnapshotRegistry(snapshotRegistry).
            setSessionTimeoutNs(1000).
            setFeatureControlManager(featureControl).
            setBrokerUncleanShutdownHandler((brokerId, records) -> { }).
            build();
        clusterControl.activate();
        for (int i = 0; i < numUsableBrokers; i++) {
            RegisterBrokerRecord brokerRecord =
                new RegisterBrokerRecord().setBrokerEpoch(100).setBrokerId(i);
            brokerRecord.endPoints().add(new BrokerEndpoint().
                setSecurityProtocol(SecurityProtocol.PLAINTEXT.id).
                setPort((short) 9092).
                setName("PLAINTEXT").
                setHost("example.com"));
            clusterControl.replay(brokerRecord, 100L);
            UnfenceBrokerRecord unfenceRecord =
                new UnfenceBrokerRecord().setId(i).setEpoch(100);
            clusterControl.replay(unfenceRecord);
            clusterControl.heartbeatManager().touch(i, false, 0);
        }
        for (int i = 0; i < numUsableBrokers; i++) {
            assertTrue(clusterControl.isUnfenced(i),
                String.format("broker %d was not unfenced.", i));
        }
        for (int i = 0; i < 100; i++) {
            List<PartitionAssignment> results = clusterControl.replicaPlacer().place(
                    new PlacementSpec(0,
                            1,
                            (short) 3),
                    new ClusterDescriber() {
                        @Override
                        public Iterator<UsableBroker> usableBrokers() {
                            return clusterControl.usableBrokers();
                        }

                        @Override
                        public Uuid defaultDir(int brokerId) {
                            return DirectoryId.UNASSIGNED;
                        }
                    }
            ).assignments();
            HashSet<Integer> seen = new HashSet<>();
            for (Integer result : results.get(0).replicas()) {
                assertTrue(result >= 0);
                assertTrue(result < numUsableBrokers);
                assertTrue(seen.add(result));
            }
        }
    }

    @ParameterizedTest
    @EnumSource(value = MetadataVersion.class, names = {"IBP_3_3_IV2", "IBP_3_3_IV3"})
    public void testRegistrationsToRecords(MetadataVersion metadataVersion) {
        MockTime time = new MockTime(0, 0, 0);
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(new LogContext());
        FeatureControlManager featureControl = new FeatureControlManager.Builder().
            setSnapshotRegistry(snapshotRegistry).
            setQuorumFeatures(new QuorumFeatures(0,
                QuorumFeatures.defaultFeatureMap(true),
                Collections.singletonList(0))).
            setMetadataVersion(metadataVersion).
            build();
        ClusterControlManager clusterControl = new ClusterControlManager.Builder().
            setTime(time).
            setSnapshotRegistry(snapshotRegistry).
            setSessionTimeoutNs(1000).
            setFeatureControlManager(featureControl).
            setBrokerUncleanShutdownHandler((brokerId, records) -> { }).
            build();
        clusterControl.activate();
        assertFalse(clusterControl.isUnfenced(0));
        for (int i = 0; i < 3; i++) {
            RegisterBrokerRecord brokerRecord = new RegisterBrokerRecord().
                setBrokerEpoch(100).setBrokerId(i).setRack(null);
            brokerRecord.endPoints().add(new BrokerEndpoint().
                setSecurityProtocol(SecurityProtocol.PLAINTEXT.id).
                setPort((short) 9092 + i).
                setName("PLAINTEXT").
                setHost("example.com"));
            clusterControl.replay(brokerRecord, 100L);
        }
        for (int i = 0; i < 2; i++) {
            UnfenceBrokerRecord unfenceBrokerRecord =
                new UnfenceBrokerRecord().setId(i).setEpoch(100);
            clusterControl.replay(unfenceBrokerRecord);
        }
        BrokerRegistrationChangeRecord registrationChangeRecord =
            new BrokerRegistrationChangeRecord().
                setBrokerId(0).
                setBrokerEpoch(100).
                setInControlledShutdown(BrokerRegistrationInControlledShutdownChange.
                    IN_CONTROLLED_SHUTDOWN.value());
        clusterControl.replay(registrationChangeRecord);
        short expectedVersion = metadataVersion.registerBrokerRecordVersion();

        ImageWriterOptions options = new ImageWriterOptions.Builder().
                setMetadataVersion(metadataVersion).
                setLossHandler(__ -> { }).
                build();
        assertEquals(new ApiMessageAndVersion(new RegisterBrokerRecord().
                setBrokerEpoch(100).setBrokerId(0).setRack(null).
                setEndPoints(new BrokerEndpointCollection(Collections.singleton(
                    new BrokerEndpoint().setSecurityProtocol(SecurityProtocol.PLAINTEXT.id).
                        setPort((short) 9092).
                        setName("PLAINTEXT").
                        setHost("example.com")).iterator())).
                setInControlledShutdown(metadataVersion.isInControlledShutdownStateSupported()).
                setFenced(false), expectedVersion),
            clusterControl.brokerRegistrations().get(0).toRecord(options));
        assertEquals(new ApiMessageAndVersion(new RegisterBrokerRecord().
                setBrokerEpoch(100).setBrokerId(1).setRack(null).
                setEndPoints(new BrokerEndpointCollection(Collections.singleton(
                    new BrokerEndpoint().setSecurityProtocol(SecurityProtocol.PLAINTEXT.id).
                        setPort((short) 9093).
                        setName("PLAINTEXT").
                        setHost("example.com")).iterator())).
                setFenced(false), expectedVersion),
            clusterControl.brokerRegistrations().get(1).toRecord(options));
        assertEquals(new ApiMessageAndVersion(new RegisterBrokerRecord().
                setBrokerEpoch(100).setBrokerId(2).setRack(null).
                setEndPoints(new BrokerEndpointCollection(Collections.singleton(
                    new BrokerEndpoint().setSecurityProtocol(SecurityProtocol.PLAINTEXT.id).
                        setPort((short) 9094).
                        setName("PLAINTEXT").
                        setHost("example.com")).iterator())).
                        setFenced(true), expectedVersion),
            clusterControl.brokerRegistrations().get(2).toRecord(options));
    }

    @Test
    public void testRegistrationWithUnsupportedMetadataVersion() {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(new LogContext());
        FeatureControlManager featureControl = new FeatureControlManager.Builder().
                setSnapshotRegistry(snapshotRegistry).
                setQuorumFeatures(new QuorumFeatures(0,
                        Collections.singletonMap(MetadataVersion.FEATURE_NAME, VersionRange.of(
                                MetadataVersion.IBP_3_1_IV0.featureLevel(),
                                MetadataVersion.IBP_3_3_IV0.featureLevel())),
                        Collections.singletonList(0))).
                setMetadataVersion(MetadataVersion.IBP_3_3_IV0).
                build();
        ClusterControlManager clusterControl = new ClusterControlManager.Builder().
                setClusterId("fPZv1VBsRFmnlRvmGcOW9w").
                setTime(new MockTime(0, 0, 0)).
                setSnapshotRegistry(snapshotRegistry).
                setFeatureControlManager(featureControl).
                setBrokerUncleanShutdownHandler((brokerId, records) -> { }).
                build();
        clusterControl.activate();

        assertEquals("Unable to register because the broker does not support version 4 of " +
            "metadata.version. It wants a version between 1 and 1, inclusive.",
            assertThrows(UnsupportedVersionException.class,
                () -> clusterControl.registerBroker(
                    new BrokerRegistrationRequestData().
                        setClusterId("fPZv1VBsRFmnlRvmGcOW9w").
                        setBrokerId(0).
                        setRack(null).
                        setIncarnationId(Uuid.fromString("0H4fUu1xQEKXFYwB1aBjhg")),
                    123L,
                    featureControl.finalizedFeatures(Long.MAX_VALUE),
                    (short) 1)).getMessage());

        assertEquals("Unable to register because the broker does not support version 4 of " +
            "metadata.version. It wants a version between 7 and 7, inclusive.",
            assertThrows(UnsupportedVersionException.class,
                () -> clusterControl.registerBroker(
                    new BrokerRegistrationRequestData().
                        setClusterId("fPZv1VBsRFmnlRvmGcOW9w").
                        setBrokerId(0).
                        setRack(null).
                        setFeatures(new BrokerRegistrationRequestData.FeatureCollection(
                                Collections.singleton(new BrokerRegistrationRequestData.Feature().
                                    setName(MetadataVersion.FEATURE_NAME).
                                    setMinSupportedVersion(MetadataVersion.IBP_3_3_IV3.featureLevel()).
                                    setMaxSupportedVersion(MetadataVersion.IBP_3_3_IV3.featureLevel())).iterator())).
                        setIncarnationId(Uuid.fromString("0H4fUu1xQEKXFYwB1aBjhg")),
                    123L,
                    featureControl.finalizedFeatures(Long.MAX_VALUE),
                    (short) 1)).getMessage());
    }

    @Test
    public void testRegisterControlWithOlderMetadataVersion() {
        FeatureControlManager featureControl = new FeatureControlManager.Builder().
            setMetadataVersion(MetadataVersion.IBP_3_3_IV0).
            build();
        ClusterControlManager clusterControl = new ClusterControlManager.Builder().
            setClusterId("fPZv1VBsRFmnlRvmGcOW9w").
            setFeatureControlManager(featureControl).
            setBrokerUncleanShutdownHandler((brokerId, records) -> { }).
            build();
        clusterControl.activate();
        assertEquals("The current MetadataVersion is too old to support controller registrations.",
            assertThrows(UnsupportedVersionException.class, () -> clusterControl.registerController(
                new ControllerRegistrationRequestData().setControllerId(1))).getMessage());
    }

    @Test
    public void testRegisterWithDuplicateDirectoryId() {
        ClusterControlManager clusterControl = new ClusterControlManager.Builder().
                setClusterId("QzZZEtC7SxucRM29Xdzijw").
                setFeatureControlManager(new FeatureControlManager.Builder().build()).
                setBrokerUncleanShutdownHandler((brokerId, records) -> { }).
                build();
        RegisterBrokerRecord brokerRecord = new RegisterBrokerRecord().setBrokerEpoch(100).setBrokerId(0).setLogDirs(asList(
                Uuid.fromString("yJGxmjfbQZSVFAlNM3uXZg"),
                Uuid.fromString("Mj3CW3OSRi29cFeNJlXuAQ")
        ));
        brokerRecord.endPoints().add(new BrokerEndpoint().setSecurityProtocol(SecurityProtocol.PLAINTEXT.id).setPort((short) 9092).setName("PLAINTEXT").setHost("127.0.0.1"));
        clusterControl.replay(brokerRecord, 100L);
        clusterControl.activate();

        assertDoesNotThrow(() ->
            registerNewBrokerWithDirs(clusterControl, 0, asList(Uuid.fromString("yJGxmjfbQZSVFAlNM3uXZg"), Uuid.fromString("Mj3CW3OSRi29cFeNJlXuAQ"))),
            "it should be possible to re-register the same broker with the same directories"
        );
        assertEquals("No directories specified in request", assertThrows(InvalidRegistrationException.class, () ->
                registerNewBrokerWithDirs(clusterControl, 1, Collections.emptyList())
        ).getMessage());
        assertEquals("Broker 0 is already registered with directory Mj3CW3OSRi29cFeNJlXuAQ", assertThrows(InvalidRegistrationException.class, () ->
                registerNewBrokerWithDirs(clusterControl, 1, asList(Uuid.fromString("TyNK6XSSQJaJc2q9uflNHg"), Uuid.fromString("Mj3CW3OSRi29cFeNJlXuAQ")))
        ).getMessage());
        assertEquals("Reserved directory ID in request", assertThrows(InvalidRegistrationException.class, () ->
                registerNewBrokerWithDirs(clusterControl, 1, asList(Uuid.fromString("TyNK6XSSQJaJc2q9uflNHg"), DirectoryId.UNASSIGNED))
        ).getMessage());
        assertEquals("Duplicate directory ID in request", assertThrows(InvalidRegistrationException.class, () ->
                registerNewBrokerWithDirs(clusterControl, 1, asList(Uuid.fromString("aR6lssMrSeyXRf65hiUovQ"), Uuid.fromString("aR6lssMrSeyXRf65hiUovQ")))
        ).getMessage());
    }

    void registerNewBrokerWithDirs(ClusterControlManager clusterControl, int brokerId, List<Uuid> dirs) {
        BrokerRegistrationRequestData data = new BrokerRegistrationRequestData().setBrokerId(brokerId)
                .setClusterId(TestUtils.fieldValue(clusterControl, ClusterControlManager.class, "clusterId"))
                .setIncarnationId(Uuid.randomUuid()).setLogDirs(dirs);
        FinalizedControllerFeatures finalizedFeatures = new FinalizedControllerFeatures(Collections.emptyMap(), 456L);
        ControllerResult<BrokerRegistrationReply> result = clusterControl.registerBroker(data, 123L, finalizedFeatures, (short) 1);
        RecordTestUtils.replayAll(clusterControl, result.records());
    }

    @Test
    public void testHasOnlineDir() {
        ClusterControlManager clusterControl = new ClusterControlManager.Builder().
                setClusterId("pjvUwj3ZTEeSVQmUiH3IJw").
                setFeatureControlManager(new FeatureControlManager.Builder().build()).
                setBrokerUncleanShutdownHandler((brokerId, records) -> { }).
                build();
        clusterControl.activate();
        registerNewBrokerWithDirs(clusterControl, 1, asList(Uuid.fromString("dir1SEbpRuG1dcpTRGOvJw"), Uuid.fromString("dir2xaEwR2m3JHTiy7PWwA")));
        assertTrue(clusterControl.registration(1).hasOnlineDir(Uuid.fromString("dir1SEbpRuG1dcpTRGOvJw")));
        assertTrue(clusterControl.hasOnlineDir(1, Uuid.fromString("dir1SEbpRuG1dcpTRGOvJw")));
        assertTrue(clusterControl.hasOnlineDir(1, Uuid.fromString("dir2xaEwR2m3JHTiy7PWwA")));
        assertTrue(clusterControl.hasOnlineDir(1, DirectoryId.UNASSIGNED));
        assertTrue(clusterControl.hasOnlineDir(1, DirectoryId.MIGRATING));
        assertFalse(clusterControl.hasOnlineDir(1, Uuid.fromString("otherAA1QFK4U1GWzkjZ5A")));
        assertFalse(clusterControl.hasOnlineDir(77, Uuid.fromString("8xVRVs6UQHGVonA9SRYseQ")));
        assertFalse(clusterControl.hasOnlineDir(1, DirectoryId.LOST));
    }

    @Test
    public void testDefaultDir() {
        ClusterControlManager clusterControl = new ClusterControlManager.Builder().
                setClusterId("pjvUwj3ZTEeSVQmUiH3IJw").
                setFeatureControlManager(new FeatureControlManager.Builder().build()).
                setBrokerUncleanShutdownHandler((brokerId, records) -> { }).
                build();
        clusterControl.activate();
        RegisterBrokerRecord brokerRecord = new RegisterBrokerRecord().setBrokerEpoch(100).setBrokerId(1).setLogDirs(Collections.emptyList());
        brokerRecord.endPoints().add(new BrokerEndpoint().setSecurityProtocol(SecurityProtocol.PLAINTEXT.id).setPort((short) 9092).setName("PLAINTEXT").setHost("127.0.0.1"));
        clusterControl.replay(brokerRecord, 100L);
        registerNewBrokerWithDirs(clusterControl, 2, asList(Uuid.fromString("singleOnlineDirectoryA")));
        registerNewBrokerWithDirs(clusterControl, 3, asList(Uuid.fromString("s4fRmyNFSH6J0vI8AVA5ew"), Uuid.fromString("UbtxBcqYSnKUEMcnTyZFWw")));
        assertEquals(DirectoryId.MIGRATING, clusterControl.defaultDir(1));
        assertEquals(Uuid.fromString("singleOnlineDirectoryA"), clusterControl.defaultDir(2));
        assertEquals(DirectoryId.UNASSIGNED, clusterControl.defaultDir(3));
        assertEquals(DirectoryId.UNASSIGNED, clusterControl.defaultDir(4));
    }
}
