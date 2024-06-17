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

import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.image.writer.RecordListWriter;
import org.apache.kafka.metadata.RecordTestUtils;
import org.apache.kafka.metadata.migration.ZkMigrationState;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Timeout(value = 40)
public class FeaturesImageTest {
    public final static FeaturesImage IMAGE1;
    public final static List<ApiMessageAndVersion> DELTA1_RECORDS;
    final static FeaturesDelta DELTA1;
    final static FeaturesImage IMAGE2;
    final static List<ApiMessageAndVersion> DELTA2_RECORDS;
    final static FeaturesDelta DELTA2;
    final static FeaturesImage IMAGE3;

    static {
        Map<String, Short> map1 = new HashMap<>();
        map1.put("foo", (short) 2);
        map1.put("bar", (short) 1);
        IMAGE1 = new FeaturesImage(map1, MetadataVersion.latestTesting(), ZkMigrationState.NONE);

        DELTA1_RECORDS = new ArrayList<>();
        // change feature level
        DELTA1_RECORDS.add(new ApiMessageAndVersion(new FeatureLevelRecord().
            setName("foo").setFeatureLevel((short) 3),
            (short) 0));
        // remove feature
        DELTA1_RECORDS.add(new ApiMessageAndVersion(new FeatureLevelRecord().
            setName("bar").setFeatureLevel((short) 0),
            (short) 0));
        // add feature
        DELTA1_RECORDS.add(new ApiMessageAndVersion(new FeatureLevelRecord().
            setName("baz").setFeatureLevel((short) 8),
            (short) 0));

        DELTA1 = new FeaturesDelta(IMAGE1);
        RecordTestUtils.replayAll(DELTA1, DELTA1_RECORDS);

        Map<String, Short> map2 = new HashMap<>();
        map2.put("foo", (short) 3);
        map2.put("baz", (short) 8);
        IMAGE2 = new FeaturesImage(map2, MetadataVersion.latestTesting(), ZkMigrationState.NONE);

        DELTA2_RECORDS = new ArrayList<>();
        // remove all features
        DELTA2_RECORDS.add(new ApiMessageAndVersion(new FeatureLevelRecord().
            setName("foo").setFeatureLevel((short) 0),
            (short) 0));
        DELTA2_RECORDS.add(new ApiMessageAndVersion(new FeatureLevelRecord().
            setName("baz").setFeatureLevel((short) 0),
            (short) 0));
        // add feature back with different feature level
        DELTA2_RECORDS.add(new ApiMessageAndVersion(new FeatureLevelRecord().
            setName("bar").setFeatureLevel((short) 1),
            (short) 0));

        DELTA2 = new FeaturesDelta(IMAGE2);
        RecordTestUtils.replayAll(DELTA2, DELTA2_RECORDS);

        Map<String, Short> map3 = Collections.singletonMap("bar", (short) 1);
        IMAGE3 = new FeaturesImage(map3, MetadataVersion.latestTesting(), ZkMigrationState.NONE);
    }

    @Test
    public void testEmptyImageRoundTrip() {
        testToImage(FeaturesImage.EMPTY);
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
        image.write(writer, new ImageWriterOptions.Builder().setMetadataVersion(image.metadataVersion()).build());
        return writer.records();
    }

    @Test
    public void testEmpty() {
        assertTrue(FeaturesImage.EMPTY.isEmpty());
        assertFalse(new FeaturesImage(Collections.singletonMap("foo", (short) 1),
            FeaturesImage.EMPTY.metadataVersion(), FeaturesImage.EMPTY.zkMigrationState()).isEmpty());
        assertFalse(new FeaturesImage(FeaturesImage.EMPTY.finalizedVersions(),
            MetadataVersion.IBP_3_3_IV0, FeaturesImage.EMPTY.zkMigrationState()).isEmpty());
        assertFalse(new FeaturesImage(FeaturesImage.EMPTY.finalizedVersions(),
            FeaturesImage.EMPTY.metadataVersion(), ZkMigrationState.MIGRATION).isEmpty());
    }
}
