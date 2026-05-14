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
package org.apache.kafka.image;

import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.BrokerRegistrationChangeRecord;
import org.apache.kafka.common.metadata.FenceBrokerRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord.BrokerEndpoint;
import org.apache.kafka.common.metadata.RegisterBrokerRecord.BrokerEndpointCollection;
import org.apache.kafka.common.metadata.RegisterBrokerRecord.BrokerFeature;
import org.apache.kafka.common.metadata.RegisterBrokerRecord.BrokerFeatureCollection;
import org.apache.kafka.common.metadata.RegisterControllerRecord;
import org.apache.kafka.common.metadata.RegisterControllerRecord.ControllerEndpoint;
import org.apache.kafka.common.metadata.RegisterControllerRecord.ControllerEndpointCollection;
import org.apache.kafka.common.metadata.UnfenceBrokerRecord;
import org.apache.kafka.common.metadata.UnregisterBrokerRecord;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.metadata.BrokerRegistration;
import org.apache.kafka.metadata.BrokerRegistrationInControlledShutdownChange;
import org.apache.kafka.metadata.ControllerRegistration;
import org.apache.kafka.metadata.RecordTestUtils;
import org.apache.kafka.metadata.VersionRange;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static org.apache.kafka.common.metadata.MetadataRecordType.BROKER_REGISTRATION_CHANGE_RECORD;
import static org.apache.kafka.common.metadata.MetadataRecordType.FENCE_BROKER_RECORD;
import static org.apache.kafka.common.metadata.MetadataRecordType.REGISTER_BROKER_RECORD;
import static org.apache.kafka.common.metadata.MetadataRecordType.UNFENCE_BROKER_RECORD;

public final class ClusterImageFixtures {

    public static final ClusterImage IMAGE1 = new ClusterImage(
        Map.of(
            0, new BrokerRegistration.Builder().
                setId(0).
                setEpoch(1000).
                setIncarnationId(Uuid.fromString("vZKYST0pSA2HO5x_6hoO2Q")).
                setListeners(List.of(new Endpoint("PLAINTEXT", SecurityProtocol.PLAINTEXT, "localhost", 9092))).
                setSupportedFeatures(Map.of("foo", VersionRange.of((short) 1, (short) 3))).
                setRack(Optional.empty()).
                setFenced(true).
                setInControlledShutdown(false).build(),
            1, new BrokerRegistration.Builder().
                setId(1).
                setEpoch(1001).
                setIncarnationId(Uuid.fromString("U52uRe20RsGI0RvpcTx33Q")).
                setListeners(List.of(new Endpoint("PLAINTEXT", SecurityProtocol.PLAINTEXT, "localhost", 9093))).
                setSupportedFeatures(Map.of("foo", VersionRange.of((short) 1, (short) 3))).
                setRack(Optional.empty()).
                setFenced(false).
                setInControlledShutdown(false).build(),
            2, new BrokerRegistration.Builder().
                setId(2).
                setEpoch(123).
                setIncarnationId(Uuid.fromString("hr4TVh3YQiu3p16Awkka6w")).
                setListeners(List.of(new Endpoint("PLAINTEXT", SecurityProtocol.PLAINTEXT, "localhost", 9094))).
                setSupportedFeatures(Map.of()).
                setRack(Optional.of("arack")).
                setFenced(false).
                setInControlledShutdown(false).build()
        ),
        Map.of(
            1000, new ControllerRegistration.Builder().
                setId(1000).
                setIncarnationId(Uuid.fromString("9ABu6HEgRuS-hjHLgC4cHw")).
                setZkMigrationReady(false).
                setListeners(Map.of("PLAINTEXT",
                    new Endpoint("PLAINTEXT", SecurityProtocol.PLAINTEXT, "localhost", 19092))).
                setSupportedFeatures(Map.of()).build()
        )
    );

    public static final List<ApiMessageAndVersion> DELTA1_RECORDS = List.of(
        // unfence b0
        new ApiMessageAndVersion(new UnfenceBrokerRecord().
            setId(0).setEpoch(1000), UNFENCE_BROKER_RECORD.highestSupportedVersion()),
        // fence b1
        new ApiMessageAndVersion(new FenceBrokerRecord().
            setId(1).setEpoch(1001), FENCE_BROKER_RECORD.highestSupportedVersion()),
        // mark b0 in controlled shutdown
        new ApiMessageAndVersion(new BrokerRegistrationChangeRecord().
            setBrokerId(0).setBrokerEpoch(1000).setInControlledShutdown(
                BrokerRegistrationInControlledShutdownChange.IN_CONTROLLED_SHUTDOWN.value()),
            BROKER_REGISTRATION_CHANGE_RECORD.highestSupportedVersion()),
        // unregister b2
        new ApiMessageAndVersion(new UnregisterBrokerRecord().
            setBrokerId(2).setBrokerEpoch(123),
            (short) 0),
        new ApiMessageAndVersion(new RegisterControllerRecord().
            setControllerId(1001).
            setIncarnationId(Uuid.fromString("FdEHF-IqScKfYyjZ1CjfNQ")).
            setZkMigrationReady(true).
            setEndPoints(new ControllerEndpointCollection(List.of(new ControllerEndpoint().
                setHost("localhost").
                setName("PLAINTEXT").
                setPort(19093).
                setSecurityProtocol(SecurityProtocol.PLAINTEXT.id)).iterator())),
            (short) 0)
    );

    public static final ClusterDelta DELTA1 = RecordTestUtils.replayAll(
        new ClusterDelta(IMAGE1),
        DELTA1_RECORDS
    );

    public static final ClusterImage IMAGE2 = new ClusterImage(
        Map.of(
            0, new BrokerRegistration.Builder().
                setId(0).
                setEpoch(1000).
                setIncarnationId(Uuid.fromString("vZKYST0pSA2HO5x_6hoO2Q")).
                setListeners(List.of(new Endpoint("PLAINTEXT", SecurityProtocol.PLAINTEXT, "localhost", 9092))).
                setSupportedFeatures(Map.of("foo", VersionRange.of((short) 1, (short) 3))).
                setRack(Optional.empty()).
                setFenced(false).
                setInControlledShutdown(true).build(),
            1, new BrokerRegistration.Builder().
                setId(1).
                setEpoch(1001).
                setIncarnationId(Uuid.fromString("U52uRe20RsGI0RvpcTx33Q")).
                setListeners(List.of(new Endpoint("PLAINTEXT", SecurityProtocol.PLAINTEXT, "localhost", 9093))).
                setSupportedFeatures(Map.of("foo", VersionRange.of((short) 1, (short) 3))).
                setRack(Optional.empty()).
                setFenced(true).
                setInControlledShutdown(false).build()
        ),
        Map.of(
            1000, IMAGE1.controllers().get(1000),
            1001, new ControllerRegistration.Builder().
                setId(1001).
                setIncarnationId(Uuid.fromString("FdEHF-IqScKfYyjZ1CjfNQ")).
                setZkMigrationReady(true).
                setListeners(Map.of("PLAINTEXT",
                    new Endpoint("PLAINTEXT", SecurityProtocol.PLAINTEXT, "localhost", 19093))).
                setSupportedFeatures(Map.of()).build()
        )
    );

    public static final List<ApiMessageAndVersion> DELTA2_RECORDS = Stream.concat(
        DELTA1_RECORDS.stream(),
        Stream.of(
            // fence b0
            new ApiMessageAndVersion(new FenceBrokerRecord().
                setId(0).setEpoch(1000), FENCE_BROKER_RECORD.highestSupportedVersion()),
            // unfence b1
            new ApiMessageAndVersion(new UnfenceBrokerRecord().
                setId(1).setEpoch(1001), UNFENCE_BROKER_RECORD.highestSupportedVersion()),
            // mark b0 as not in controlled shutdown
            new ApiMessageAndVersion(new BrokerRegistrationChangeRecord().
                setBrokerId(0).setBrokerEpoch(1000).setInControlledShutdown(
                    BrokerRegistrationInControlledShutdownChange.NONE.value()),
                BROKER_REGISTRATION_CHANGE_RECORD.highestSupportedVersion()),
            // re-register b2
            new ApiMessageAndVersion(new RegisterBrokerRecord().
                setBrokerId(2).setIsMigratingZkBroker(true).setIncarnationId(Uuid.fromString("Am5Yse7GQxaw0b2alM74bP")).
                setBrokerEpoch(1002).setEndPoints(new BrokerEndpointCollection(
                    List.of(new BrokerEndpoint().setName("PLAINTEXT").setHost("localhost").
                        setPort(9094).setSecurityProtocol((short) 0)).iterator())).
                setFeatures(new BrokerFeatureCollection(
                    Set.of(new BrokerFeature().
                        setName(MetadataVersion.FEATURE_NAME).
                        setMinSupportedVersion(MetadataVersion.MINIMUM_VERSION.featureLevel()).
                        setMaxSupportedVersion(MetadataVersion.IBP_3_6_IV0.featureLevel())).iterator())).
                setRack("rack3"),
                REGISTER_BROKER_RECORD.highestSupportedVersion())
        )
    ).toList();

    public static final ClusterDelta DELTA2 = RecordTestUtils.replayAll(
        new ClusterDelta(IMAGE2),
        DELTA2_RECORDS
    );

    public static final ClusterImage IMAGE3 = new ClusterImage(
        Map.of(
            0, new BrokerRegistration.Builder().
                setId(0).
                setEpoch(1000).
                setIncarnationId(Uuid.fromString("vZKYST0pSA2HO5x_6hoO2Q")).
                setListeners(List.of(new Endpoint("PLAINTEXT", SecurityProtocol.PLAINTEXT, "localhost", 9092))).
                setSupportedFeatures(Map.of("foo", VersionRange.of((short) 1, (short) 3))).
                setRack(Optional.empty()).
                setFenced(true).
                setInControlledShutdown(true).build(),
            1, new BrokerRegistration.Builder().
                setId(1).
                setEpoch(1001).
                setIncarnationId(Uuid.fromString("U52uRe20RsGI0RvpcTx33Q")).
                setListeners(List.of(new Endpoint("PLAINTEXT", SecurityProtocol.PLAINTEXT, "localhost", 9093))).
                setSupportedFeatures(Map.of("foo", VersionRange.of((short) 1, (short) 3))).
                setRack(Optional.empty()).
                setFenced(false).
                setInControlledShutdown(false).build(),
            2, new BrokerRegistration.Builder().
                setId(2).
                setEpoch(1002).
                setIncarnationId(Uuid.fromString("Am5Yse7GQxaw0b2alM74bP")).
                setListeners(List.of(new Endpoint("PLAINTEXT", SecurityProtocol.PLAINTEXT, "localhost", 9094))).
                setSupportedFeatures(Map.of("metadata.version",
                    VersionRange.of(MetadataVersion.MINIMUM_VERSION.featureLevel(), MetadataVersion.IBP_3_6_IV0.featureLevel()))).
                setRack(Optional.of("rack3")).
                setFenced(true).
                setIsMigratingZkBroker(true).build()
        ),
        IMAGE2.controllers()
    );

    private ClusterImageFixtures() {
    }
}
