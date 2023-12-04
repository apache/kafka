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
import org.apache.kafka.common.metadata.RegisterControllerRecord;
import org.apache.kafka.common.metadata.RegisterControllerRecord.ControllerEndpoint;
import org.apache.kafka.common.metadata.RegisterControllerRecord.ControllerEndpointCollection;
import org.apache.kafka.common.metadata.UnfenceBrokerRecord;
import org.apache.kafka.common.metadata.UnregisterBrokerRecord;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.image.writer.RecordListWriter;
import org.apache.kafka.metadata.BrokerRegistration;
import org.apache.kafka.metadata.BrokerRegistrationInControlledShutdownChange;
import org.apache.kafka.metadata.ControllerRegistration;
import org.apache.kafka.metadata.RecordTestUtils;
import org.apache.kafka.metadata.VersionRange;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.kafka.common.metadata.MetadataRecordType.FENCE_BROKER_RECORD;
import static org.apache.kafka.common.metadata.MetadataRecordType.UNFENCE_BROKER_RECORD;
import static org.junit.jupiter.api.Assertions.assertEquals;


@Timeout(value = 40)
public class ClusterImageTest {

    public final static ClusterImage IMAGE1;

    static final List<ApiMessageAndVersion> DELTA1_RECORDS;

    final static ClusterDelta DELTA1;

    final static ClusterImage IMAGE2;

    static {
        Map<Integer, BrokerRegistration> map1 = new HashMap<>();
        map1.put(0, new BrokerRegistration.Builder().
            setId(0).
            setEpoch(1000).
            setIncarnationId(Uuid.fromString("vZKYST0pSA2HO5x_6hoO2Q")).
            setListeners(Arrays.asList(new Endpoint("PLAINTEXT", SecurityProtocol.PLAINTEXT, "localhost", 9092))).
            setSupportedFeatures(Collections.singletonMap("foo", VersionRange.of((short) 1, (short) 3))).
            setRack(Optional.empty()).
            setFenced(true).
            setInControlledShutdown(false).build());
        map1.put(1, new BrokerRegistration.Builder().
            setId(1).
            setEpoch(1001).
            setIncarnationId(Uuid.fromString("U52uRe20RsGI0RvpcTx33Q")).
            setListeners(Arrays.asList(new Endpoint("PLAINTEXT", SecurityProtocol.PLAINTEXT, "localhost", 9093))).
            setSupportedFeatures(Collections.singletonMap("foo", VersionRange.of((short) 1, (short) 3))).
            setRack(Optional.empty()).
            setFenced(false).
            setInControlledShutdown(false).build());
        map1.put(2, new BrokerRegistration.Builder().
            setId(2).
            setEpoch(123).
            setIncarnationId(Uuid.fromString("hr4TVh3YQiu3p16Awkka6w")).
            setListeners(Arrays.asList(new Endpoint("PLAINTEXT", SecurityProtocol.PLAINTEXT, "localhost", 9093))).
            setSupportedFeatures(Collections.emptyMap()).
            setRack(Optional.of("arack")).
            setFenced(false).
            setInControlledShutdown(false).build());
        Map<Integer, ControllerRegistration> cmap1 = new HashMap<>();
        cmap1.put(1000, new ControllerRegistration.Builder().
            setId(1000).
            setIncarnationId(Uuid.fromString("9ABu6HEgRuS-hjHLgC4cHw")).
            setZkMigrationReady(false).
            setListeners(Collections.singletonMap("PLAINTEXT",
                    new Endpoint("PLAINTEXT", SecurityProtocol.PLAINTEXT, "localhost", 19092))).
            setSupportedFeatures(Collections.emptyMap()).build());
        IMAGE1 = new ClusterImage(map1, cmap1);

        DELTA1_RECORDS = new ArrayList<>();
        DELTA1_RECORDS.add(new ApiMessageAndVersion(new UnfenceBrokerRecord().
            setId(0).setEpoch(1000), UNFENCE_BROKER_RECORD.highestSupportedVersion()));
        DELTA1_RECORDS.add(new ApiMessageAndVersion(new FenceBrokerRecord().
            setId(1).setEpoch(1001), FENCE_BROKER_RECORD.highestSupportedVersion()));
        DELTA1_RECORDS.add(new ApiMessageAndVersion(new BrokerRegistrationChangeRecord().
            setBrokerId(0).setBrokerEpoch(1000).setInControlledShutdown(
            BrokerRegistrationInControlledShutdownChange.IN_CONTROLLED_SHUTDOWN.value()),
            (short) 0));
        DELTA1_RECORDS.add(new ApiMessageAndVersion(new UnregisterBrokerRecord().
            setBrokerId(2).setBrokerEpoch(123),
            (short) 0));

        ControllerEndpointCollection endpointsFor1001 = new ControllerEndpointCollection();
        endpointsFor1001.add(new ControllerEndpoint().
            setHost("localhost").
            setName("PLAINTEXT").
            setPort(19093).
            setSecurityProtocol(SecurityProtocol.PLAINTEXT.id));
        DELTA1_RECORDS.add(new ApiMessageAndVersion(new RegisterControllerRecord().
            setControllerId(1001).
            setIncarnationId(Uuid.fromString("FdEHF-IqScKfYyjZ1CjfNQ")).
            setZkMigrationReady(true).
            setEndPoints(endpointsFor1001),
            (short) 0));

        DELTA1 = new ClusterDelta(IMAGE1);
        RecordTestUtils.replayAll(DELTA1, DELTA1_RECORDS);

        Map<Integer, BrokerRegistration> map2 = new HashMap<>();
        map2.put(0, new BrokerRegistration.Builder().
            setId(0).
            setEpoch(1000).
            setIncarnationId(Uuid.fromString("vZKYST0pSA2HO5x_6hoO2Q")).
            setListeners(Arrays.asList(new Endpoint("PLAINTEXT", SecurityProtocol.PLAINTEXT, "localhost", 9092))).
            setSupportedFeatures(Collections.singletonMap("foo", VersionRange.of((short) 1, (short) 3))).
            setRack(Optional.empty()).
            setFenced(false).
            setInControlledShutdown(true).build());
        map2.put(1, new BrokerRegistration.Builder().
            setId(1).
            setEpoch(1001).
            setIncarnationId(Uuid.fromString("U52uRe20RsGI0RvpcTx33Q")).
            setListeners(Arrays.asList(new Endpoint("PLAINTEXT", SecurityProtocol.PLAINTEXT, "localhost", 9093))).
            setSupportedFeatures(Collections.singletonMap("foo", VersionRange.of((short) 1, (short) 3))).
            setRack(Optional.empty()).
            setFenced(true).
            setInControlledShutdown(false).build());
        Map<Integer, ControllerRegistration> cmap2 = new HashMap<>(cmap1);
        cmap2.put(1001, new ControllerRegistration.Builder().
            setId(1001).
            setIncarnationId(Uuid.fromString("FdEHF-IqScKfYyjZ1CjfNQ")).
            setZkMigrationReady(true).
            setListeners(Collections.singletonMap("PLAINTEXT",
                new Endpoint("PLAINTEXT", SecurityProtocol.PLAINTEXT, "localhost", 19093))).
            setSupportedFeatures(Collections.emptyMap()).build());
        IMAGE2 = new ClusterImage(map2, cmap2);
    }

    @Test
    public void testEmptyImageRoundTrip() {
        testToImage(ClusterImage.EMPTY);
    }

    @Test
    public void testImage1RoundTrip() {
        testToImage(IMAGE1);
    }

    @Test
    public void testApplyDelta1() {
        assertEquals(IMAGE2, DELTA1.apply());
        // check image1 + delta1 = image2, since records for image1 + delta1 might differ from records from image2
        List<ApiMessageAndVersion> records = getImageRecords(IMAGE1);
        records.addAll(DELTA1_RECORDS);
        testToImage(IMAGE2, records);
    }

    @Test
    public void testImage2RoundTrip() {
        testToImage(IMAGE2);
    }

    private static void testToImage(ClusterImage image) {
        testToImage(image, Optional.empty());
    }

    private static void testToImage(ClusterImage image, Optional<List<ApiMessageAndVersion>> fromRecords) {
        testToImage(image, fromRecords.orElseGet(() -> getImageRecords(image)));
    }

    private static void testToImage(ClusterImage image, List<ApiMessageAndVersion> fromRecords) {
        // test from empty image stopping each of the various intermediate images along the way
        new RecordTestUtils.TestThroughAllIntermediateImagesLeadingToFinalImageHelper<>(
            () -> ClusterImage.EMPTY,
            ClusterDelta::new
        ).test(image, fromRecords);
    }

    private static List<ApiMessageAndVersion> getImageRecords(ClusterImage image) {
        RecordListWriter writer = new RecordListWriter();
        image.write(writer, new ImageWriterOptions.Builder().build());
        return writer.records();
    }

    @Test
    public void testHandleLossOfControllerRegistrations() {
        ClusterImage testImage = new ClusterImage(Collections.emptyMap(),
            Collections.singletonMap(1000, new ControllerRegistration.Builder().
                setId(1000).
                setIncarnationId(Uuid.fromString("9ABu6HEgRuS-hjHLgC4cHw")).
                setListeners(Collections.singletonMap("PLAINTEXT",
                    new Endpoint("PLAINTEXT", SecurityProtocol.PLAINTEXT, "localhost", 19092))).
                setSupportedFeatures(Collections.emptyMap()).build()));
        RecordListWriter writer = new RecordListWriter();
        final AtomicReference<String> lossString = new AtomicReference<>("");
        testImage.write(writer, new ImageWriterOptions.Builder().
            setMetadataVersion(MetadataVersion.IBP_3_6_IV2).
            setLossHandler(loss -> lossString.compareAndSet("", loss.loss())).
                build());
        assertEquals("controller registration data", lossString.get());
    }
}
