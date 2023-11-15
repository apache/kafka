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

import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InconsistentClusterIdException;
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
import org.apache.kafka.metadata.VersionRange;
import org.apache.kafka.metadata.placement.PartitionAssignment;
import org.apache.kafka.metadata.placement.PlacementSpec;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;

import static org.apache.kafka.server.common.MetadataVersion.IBP_3_3_IV2;
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
                QuorumFeatures.defaultFeatureMap(),
                Collections.singletonList(0))).
            setMetadataVersion(MetadataVersion.latest()).
            build();
        ClusterControlManager clusterControl = new ClusterControlManager.Builder().
            setTime(time).
            setSnapshotRegistry(snapshotRegistry).
            setSessionTimeoutNs(1000).
            setFeatureControlManager(featureControl).
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
                QuorumFeatures.defaultFeatureMap(),
                Collections.singletonList(0))).
            setMetadataVersion(MetadataVersion.latest()).
            build();
        ClusterControlManager clusterControl = new ClusterControlManager.Builder().
            setClusterId("fPZv1VBsRFmnlRvmGcOW9w").
            setTime(time).
            setSnapshotRegistry(snapshotRegistry).
            setSessionTimeoutNs(1000).
            setFeatureControlManager(featureControl).
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
                QuorumFeatures.defaultFeatureMap(),
                Collections.singletonList(0))).
            setMetadataVersion(MetadataVersion.latest()).
            build();
        ClusterControlManager clusterControl = new ClusterControlManager.Builder().
            setClusterId("fPZv1VBsRFmnlRvmGcOW9w").
            setTime(time).
            setSnapshotRegistry(snapshotRegistry).
            setSessionTimeoutNs(1000).
            setFeatureControlManager(featureControl).
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
                QuorumFeatures.defaultFeatureMap(),
                Collections.singletonList(0))).
            setMetadataVersion(MetadataVersion.latest()).
            build();
        ClusterControlManager clusterControl = new ClusterControlManager.Builder().
            setClusterId("fPZv1VBsRFmnlRvmGcOW9w").
            setTime(new MockTime(0, 0, 0)).
            setSnapshotRegistry(snapshotRegistry).
            setSessionTimeoutNs(1000).
            setFeatureControlManager(featureControl).
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

    @ParameterizedTest
    @EnumSource(value = MetadataVersion.class, names = {"IBP_3_3_IV2", "IBP_3_3_IV3"})
    public void testRegisterBrokerRecordVersion(MetadataVersion metadataVersion) {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(new LogContext());
        FeatureControlManager featureControl = new FeatureControlManager.Builder().
            setSnapshotRegistry(snapshotRegistry).
            setQuorumFeatures(new QuorumFeatures(0,
                QuorumFeatures.defaultFeatureMap(),
                Collections.singletonList(0))).
            setMetadataVersion(metadataVersion).
            build();
        ClusterControlManager clusterControl = new ClusterControlManager.Builder().
            setClusterId("fPZv1VBsRFmnlRvmGcOW9w").
            setTime(new MockTime(0, 0, 0)).
            setSnapshotRegistry(snapshotRegistry).
            setSessionTimeoutNs(1000).
            setFeatureControlManager(featureControl).
            build();
        clusterControl.activate();

        ControllerResult<BrokerRegistrationReply> result = clusterControl.registerBroker(
            new BrokerRegistrationRequestData().
                setClusterId("fPZv1VBsRFmnlRvmGcOW9w").
                setBrokerId(0).
                setRack(null).
                setIncarnationId(Uuid.fromString("0H4fUu1xQEKXFYwB1aBjhg")),
            123L,
            new FinalizedControllerFeatures(Collections.emptyMap(), 456L),
            (short) 1);

        short expectedVersion = metadataVersion.registerBrokerRecordVersion();

        assertEquals(
            Arrays.asList(new ApiMessageAndVersion(new RegisterBrokerRecord().
                setBrokerEpoch(123L).
                setBrokerId(0).
                setRack(null).
                setIncarnationId(Uuid.fromString("0H4fUu1xQEKXFYwB1aBjhg")).
                setFenced(true).
                setFeatures(new RegisterBrokerRecord.BrokerFeatureCollection(Arrays.asList(
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
                QuorumFeatures.defaultFeatureMap(),
                Collections.singletonList(0))).
            setMetadataVersion(MetadataVersion.latest()).
            build();
        ClusterControlManager clusterControl = new ClusterControlManager.Builder().
            setTime(new MockTime(0, 0, 0)).
            setSnapshotRegistry(snapshotRegistry).
            setSessionTimeoutNs(1000).
            setFeatureControlManager(featureControl).
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
                QuorumFeatures.defaultFeatureMap(),
                Collections.singletonList(0))).
            setMetadataVersion(MetadataVersion.latest()).
            build();
        ClusterControlManager clusterControl = new ClusterControlManager.Builder().
            setTime(time).
            setSnapshotRegistry(snapshotRegistry).
            setSessionTimeoutNs(1000).
            setFeatureControlManager(featureControl).
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
                    clusterControl::usableBrokers
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
                QuorumFeatures.defaultFeatureMap(),
                Collections.singletonList(0))).
            setMetadataVersion(metadataVersion).
            build();
        ClusterControlManager clusterControl = new ClusterControlManager.Builder().
            setTime(time).
            setSnapshotRegistry(snapshotRegistry).
            setSessionTimeoutNs(1000).
            setFeatureControlManager(featureControl).
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
            build();
        clusterControl.activate();
        assertEquals("The current MetadataVersion is too old to support controller registrations.",
            assertThrows(UnsupportedVersionException.class, () -> clusterControl.registerController(
                new ControllerRegistrationRequestData().setControllerId(1))).getMessage());
    }
}
