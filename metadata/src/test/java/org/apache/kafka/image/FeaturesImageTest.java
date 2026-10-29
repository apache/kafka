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

import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.image.writer.RecordListWriter;
import org.apache.kafka.metadata.RecordTestUtils;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.EligibleLeaderReplicasVersion;
import org.apache.kafka.server.common.MetadataVersion;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Timeout(value = 40)
public class FeaturesImageTest {
    public static final FeaturesImage IMAGE1 = FeaturesImageFixtures.IMAGE1;
    public static final List<ApiMessageAndVersion> DELTA1_RECORDS = FeaturesImageFixtures.DELTA1_RECORDS;
    static final FeaturesDelta DELTA1 = FeaturesImageFixtures.DELTA1;
    static final FeaturesImage IMAGE2 = FeaturesImageFixtures.IMAGE2;
    static final List<ApiMessageAndVersion> DELTA2_RECORDS = FeaturesImageFixtures.DELTA2_RECORDS;
    static final FeaturesDelta DELTA2 = FeaturesImageFixtures.DELTA2;
    static final FeaturesImage IMAGE3 = FeaturesImageFixtures.IMAGE3;

    @Test
    public void testEmptyImageRoundTrip() {
        var image = FeaturesImage.EMPTY;
        var metadataVersion = MetadataVersion.MINIMUM_VERSION;
        RecordListWriter writer = new RecordListWriter();
        image.write(writer, new ImageWriterOptions.Builder(metadataVersion).build());
        // A metadata version is required for writing, so the expected image is not actually empty
        var expectedImage = new FeaturesImage(Map.of(), metadataVersion);
        testToImage(expectedImage, writer.records());
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
    public void testImage3RoundTrip() {
        testToImage(IMAGE3);
    }

    @Test
    public void testApplyDelta2() {
        assertEquals(IMAGE3, DELTA2.apply());
        // check image2 + delta2 = image3, since records for image2 + delta2 might differ from records from image3
        List<ApiMessageAndVersion> records = getImageRecords(IMAGE2);
        records.addAll(DELTA2_RECORDS);
        testToImage(IMAGE3, records);
    }

    private static void testToImage(FeaturesImage image) {
        testToImage(image, Optional.empty());
    }

    private static void testToImage(FeaturesImage image, Optional<List<ApiMessageAndVersion>> fromRecords) {
        testToImage(image, fromRecords.orElseGet(() -> getImageRecords(image)));
    }

    private static void testToImage(FeaturesImage image, List<ApiMessageAndVersion> fromRecords) {
        // test from empty image stopping each of the various intermediate images along the way
        new RecordTestUtils.TestThroughAllIntermediateImagesLeadingToFinalImageHelper<>(
            () -> FeaturesImage.EMPTY,
            FeaturesDelta::new
        ).test(image, fromRecords);
    }

    private static List<ApiMessageAndVersion> getImageRecords(FeaturesImage image) {
        RecordListWriter writer = new RecordListWriter();
        image.write(writer, new ImageWriterOptions.Builder(image.metadataVersionOrThrow()).build());
        return writer.records();
    }

    @Test
    public void testEmpty() {
        assertTrue(FeaturesImage.EMPTY.isEmpty());
        assertFalse(new FeaturesImage(Map.of("foo", (short) 1),
            MetadataVersion.MINIMUM_VERSION).isEmpty());
        assertFalse(new FeaturesImage(FeaturesImage.EMPTY.finalizedVersions(),
            MetadataVersion.MINIMUM_VERSION).isEmpty());
    }

    @Test
    public void testElrEnabled() {
        FeaturesImage image1 = new FeaturesImage(
            Map.of(EligibleLeaderReplicasVersion.FEATURE_NAME, EligibleLeaderReplicasVersion.ELRV_0.featureLevel()),
            MetadataVersion.latestTesting()
        );
        assertFalse(image1.isElrEnabled());

        FeaturesImage image2 = new FeaturesImage(
            Map.of(EligibleLeaderReplicasVersion.FEATURE_NAME, EligibleLeaderReplicasVersion.ELRV_1.featureLevel()),
            MetadataVersion.latestTesting()
        );
        assertTrue(image2.isElrEnabled());
    }
}
