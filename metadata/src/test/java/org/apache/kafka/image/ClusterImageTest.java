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
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.image.writer.RecordListWriter;
import org.apache.kafka.metadata.ControllerRegistration;
import org.apache.kafka.metadata.RecordTestUtils;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;


@Timeout(value = 40)
public class ClusterImageTest {

    public static final ClusterImage IMAGE1 = ClusterImageFixtures.IMAGE1;

    static final List<ApiMessageAndVersion> DELTA1_RECORDS = ClusterImageFixtures.DELTA1_RECORDS;

    static final ClusterDelta DELTA1 = ClusterImageFixtures.DELTA1;

    static final ClusterImage IMAGE2 = ClusterImageFixtures.IMAGE2;

    static final List<ApiMessageAndVersion> DELTA2_RECORDS = ClusterImageFixtures.DELTA2_RECORDS;

    static final ClusterDelta DELTA2 = ClusterImageFixtures.DELTA2;

    static final ClusterImage IMAGE3 = ClusterImageFixtures.IMAGE3;

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

    @Test
    public void testApplyDelta2() {
        assertEquals(IMAGE3, DELTA2.apply());
        // check image2 + delta2 = image3, since records for image2 + delta2 might differ from records from image3
        List<ApiMessageAndVersion> records = getImageRecords(IMAGE2);
        records.addAll(DELTA2_RECORDS);
        testToImage(IMAGE3, records);
    }

    @Test
    public void testImage3RoundTrip() {
        testToImage(IMAGE3);
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
        image.write(writer, new ImageWriterOptions.Builder(MetadataVersion.latestProduction()).build());
        return writer.records();
    }

    @Test
    public void testHandleLossOfControllerRegistrations() {
        ClusterImage testImage = new ClusterImage(Map.of(),
            Map.of(1000, new ControllerRegistration.Builder().
                setId(1000).
                setIncarnationId(Uuid.fromString("9ABu6HEgRuS-hjHLgC4cHw")).
                setListeners(Map.of("PLAINTEXT",
                    new Endpoint("PLAINTEXT", SecurityProtocol.PLAINTEXT, "localhost", 19092))).
                setSupportedFeatures(Map.of()).build()));
        RecordListWriter writer = new RecordListWriter();
        final AtomicReference<String> lossString = new AtomicReference<>("");
        testImage.write(writer, new ImageWriterOptions.Builder(MetadataVersion.IBP_3_6_IV2).
            setLossHandler(loss -> lossString.compareAndSet("", loss.loss())).
                build());
        assertEquals("controller registration data", lossString.get());
    }

    @Test
    public void testBrokerEpoch() {
        assertEquals(123L, IMAGE1.brokerEpoch(2));
    }

    @Test
    public void testBrokerEpochForNonExistentBroker() {
        assertEquals(-1L, IMAGE1.brokerEpoch(20));
    }

}
