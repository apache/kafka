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
import org.apache.kafka.common.metadata.FenceBrokerRecord;
import org.apache.kafka.common.metadata.UnfenceBrokerRecord;
import org.apache.kafka.common.metadata.UnregisterBrokerRecord;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.metadata.BrokerRegistration;
import org.apache.kafka.metadata.RecordTestUtils;
import org.apache.kafka.metadata.VersionRange;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.kafka.common.metadata.MetadataRecordType.FENCE_BROKER_RECORD;
import static org.apache.kafka.common.metadata.MetadataRecordType.UNFENCE_BROKER_RECORD;
import static org.apache.kafka.common.metadata.MetadataRecordType.UNREGISTER_BROKER_RECORD;
import static org.junit.jupiter.api.Assertions.assertEquals;


@Timeout(value = 40)
public class ClusterImageTest {
    public final static ClusterImage IMAGE1;

    static final List<ApiMessageAndVersion> DELTA1_RECORDS;

    final static ClusterDelta DELTA1;

    final static ClusterImage IMAGE2;

    static {
        Map<Integer, BrokerRegistration> map1 = new HashMap<>();
        map1.put(0, new BrokerRegistration(0,
            1000,
            Uuid.fromString("vZKYST0pSA2HO5x_6hoO2Q"),
            Arrays.asList(new Endpoint("PLAINTEXT", SecurityProtocol.PLAINTEXT, "localhost", 9092)),
            Collections.singletonMap("foo", new VersionRange((short) 1, (short) 3)),
            Optional.empty(),
            true));
        map1.put(1, new BrokerRegistration(1,
            1001,
            Uuid.fromString("U52uRe20RsGI0RvpcTx33Q"),
            Arrays.asList(new Endpoint("PLAINTEXT", SecurityProtocol.PLAINTEXT, "localhost", 9093)),
            Collections.singletonMap("foo", new VersionRange((short) 1, (short) 3)),
            Optional.empty(),
            false));
        map1.put(2, new BrokerRegistration(2,
            123,
            Uuid.fromString("hr4TVh3YQiu3p16Awkka6w"),
            Arrays.asList(new Endpoint("PLAINTEXT", SecurityProtocol.PLAINTEXT, "localhost", 9093)),
            Collections.emptyMap(),
            Optional.of("arack"),
            false));
        IMAGE1 = new ClusterImage(map1);

        DELTA1_RECORDS = new ArrayList<>();
        DELTA1_RECORDS.add(new ApiMessageAndVersion(new UnfenceBrokerRecord().
            setId(0).setEpoch(1000), UNFENCE_BROKER_RECORD.highestSupportedVersion()));
        DELTA1_RECORDS.add(new ApiMessageAndVersion(new FenceBrokerRecord().
            setId(1).setEpoch(1001), FENCE_BROKER_RECORD.highestSupportedVersion()));
        DELTA1_RECORDS.add(new ApiMessageAndVersion(new UnregisterBrokerRecord().
            setBrokerId(2).setBrokerEpoch(123),
            UNREGISTER_BROKER_RECORD.highestSupportedVersion()));

        DELTA1 = new ClusterDelta(IMAGE1);
        RecordTestUtils.replayAll(DELTA1, DELTA1_RECORDS);

        Map<Integer, BrokerRegistration> map2 = new HashMap<>();
        map2.put(0, new BrokerRegistration(0,
            1000,
            Uuid.fromString("vZKYST0pSA2HO5x_6hoO2Q"),
            Arrays.asList(new Endpoint("PLAINTEXT", SecurityProtocol.PLAINTEXT, "localhost", 9092)),
            Collections.singletonMap("foo", new VersionRange((short) 1, (short) 3)),
            Optional.empty(),
            false));
        map2.put(1, new BrokerRegistration(1,
            1001,
            Uuid.fromString("U52uRe20RsGI0RvpcTx33Q"),
            Arrays.asList(new Endpoint("PLAINTEXT", SecurityProtocol.PLAINTEXT, "localhost", 9093)),
            Collections.singletonMap("foo", new VersionRange((short) 1, (short) 3)),
            Optional.empty(),
            true));
        IMAGE2 = new ClusterImage(map2);
    }

    @Test
    public void testEmptyImageRoundTrip() throws Throwable {
        testToImageAndBack(ClusterImage.EMPTY);
    }

    @Test
    public void testImage1RoundTrip() throws Throwable {
        testToImageAndBack(IMAGE1);
    }

    @Test
    public void testApplyDelta1() throws Throwable {
        assertEquals(IMAGE2, DELTA1.apply());
    }

    @Test
    public void testImage2RoundTrip() throws Throwable {
        testToImageAndBack(IMAGE2);
    }

    private void testToImageAndBack(ClusterImage image) throws Throwable {
        MockSnapshotConsumer writer = new MockSnapshotConsumer();
        image.write(writer);
        ClusterDelta delta = new ClusterDelta(ClusterImage.EMPTY);
        RecordTestUtils.replayAllBatches(delta, writer.batches());
        ClusterImage nextImage = delta.apply();
        assertEquals(image, nextImage);
    }
}
