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
import org.apache.kafka.common.metadata.RemoveFeatureLevelRecord;
import org.apache.kafka.metadata.RecordTestUtils;
import org.apache.kafka.metadata.VersionRange;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.metadata.MetadataRecordType.FEATURE_LEVEL_RECORD;
import static org.apache.kafka.common.metadata.MetadataRecordType.REMOVE_FEATURE_LEVEL_RECORD;
import static org.junit.jupiter.api.Assertions.assertEquals;


@Timeout(value = 40)
public class FeaturesImageTest {
    final static FeaturesImage IMAGE1;
    final static List<ApiMessageAndVersion> DELTA1_RECORDS;
    final static FeaturesDelta DELTA1;
    final static FeaturesImage IMAGE2;

    static {
        Map<String, VersionRange> map1 = new HashMap<>();
        map1.put("foo", new VersionRange((short) 1, (short) 2));
        map1.put("bar", new VersionRange((short) 1, (short) 1));
        map1.put("baz", new VersionRange((short) 1, (short) 8));
        IMAGE1 = new FeaturesImage(map1);

        DELTA1_RECORDS = new ArrayList<>();
        DELTA1_RECORDS.add(new ApiMessageAndVersion(new FeatureLevelRecord().
            setName("foo").setMinFeatureLevel((short) 1).setMaxFeatureLevel((short) 3),
            FEATURE_LEVEL_RECORD.highestSupportedVersion()));
        DELTA1_RECORDS.add(new ApiMessageAndVersion(new RemoveFeatureLevelRecord().
            setName("bar"), REMOVE_FEATURE_LEVEL_RECORD.highestSupportedVersion()));
        DELTA1_RECORDS.add(new ApiMessageAndVersion(new RemoveFeatureLevelRecord().
            setName("baz"), REMOVE_FEATURE_LEVEL_RECORD.highestSupportedVersion()));

        DELTA1 = new FeaturesDelta(IMAGE1);
        RecordTestUtils.replayAll(DELTA1, DELTA1_RECORDS);

        Map<String, VersionRange> map2 = new HashMap<>();
        map2.put("foo", new VersionRange((short) 1, (short) 3));
        IMAGE2 = new FeaturesImage(map2);
    }

    @Test
    public void testEmptyImageRoundTrip() throws Throwable {
        testToImageAndBack(FeaturesImage.EMPTY);
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

    private void testToImageAndBack(FeaturesImage image) throws Throwable {
        MockSnapshotConsumer writer = new MockSnapshotConsumer();
        image.write(writer);
        FeaturesDelta delta = new FeaturesDelta(FeaturesImage.EMPTY);
        RecordTestUtils.replayAllBatches(delta, writer.batches());
        FeaturesImage nextImage = delta.apply();
        assertEquals(image, nextImage);
    }
}
