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
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;


@Timeout(value = 40)
public class FeaturesImageTest {
    final static FeaturesImage IMAGE1;
    final static List<ApiMessageAndVersion> DELTA1_RECORDS;
    final static FeaturesDelta DELTA1;
    final static FeaturesImage IMAGE2;

    static {
        Map<String, Short> map1 = new HashMap<>();
        map1.put("foo", (short) 2);
        map1.put("bar", (short) 1);
        map1.put("baz", (short) 8);
        IMAGE1 = new FeaturesImage(map1, MetadataVersion.latest());

        DELTA1_RECORDS = new ArrayList<>();
        DELTA1_RECORDS.add(new ApiMessageAndVersion(new FeatureLevelRecord().
            setName("foo").setFeatureLevel((short) 3),
            (short) 0));
        DELTA1_RECORDS.add(new ApiMessageAndVersion(new FeatureLevelRecord().
            setName("bar").setFeatureLevel((short) 0),
            (short) 0));
        DELTA1_RECORDS.add(new ApiMessageAndVersion(new FeatureLevelRecord().
            setName("baz").setFeatureLevel((short) 0),
            (short) 0));

        DELTA1 = new FeaturesDelta(IMAGE1);
        RecordTestUtils.replayAll(DELTA1, DELTA1_RECORDS);

        Map<String, Short> map2 = new HashMap<>();
        map2.put("foo", (short) 3);
        IMAGE2 = new FeaturesImage(map2, MetadataVersion.latest());
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
        RecordListWriter writer = new RecordListWriter();
        image.write(writer, new ImageWriterOptions.Builder().build());
        FeaturesDelta delta = new FeaturesDelta(FeaturesImage.EMPTY);
        RecordTestUtils.replayAll(delta, writer.records());
        FeaturesImage nextImage = delta.apply();
        assertEquals(image, nextImage);
    }
}
