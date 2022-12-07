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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;


@Timeout(value = 40)
public class MetadataImageTest {
    public final static MetadataImage IMAGE1;

    public final static MetadataDelta DELTA1;

    public final static MetadataImage IMAGE2;

    static {
        IMAGE1 = new MetadataImage(
            new MetadataProvenance(100, 4, 2000),
            FeaturesImageTest.IMAGE1,
            ClusterImageTest.IMAGE1,
            TopicsImageTest.IMAGE1,
            ConfigurationsImageTest.IMAGE1,
            ClientQuotasImageTest.IMAGE1,
            ProducerIdsImageTest.IMAGE1,
            AclsImageTest.IMAGE1);

        DELTA1 = new MetadataDelta.Builder().
                setImage(IMAGE1).
                build();
        RecordTestUtils.replayAll(DELTA1, FeaturesImageTest.DELTA1_RECORDS);
        RecordTestUtils.replayAll(DELTA1, ClusterImageTest.DELTA1_RECORDS);
        RecordTestUtils.replayAll(DELTA1, TopicsImageTest.DELTA1_RECORDS);
        RecordTestUtils.replayAll(DELTA1, ConfigurationsImageTest.DELTA1_RECORDS);
        RecordTestUtils.replayAll(DELTA1, ClientQuotasImageTest.DELTA1_RECORDS);
        RecordTestUtils.replayAll(DELTA1, ProducerIdsImageTest.DELTA1_RECORDS);
        RecordTestUtils.replayAll(DELTA1, AclsImageTest.DELTA1_RECORDS);

        IMAGE2 = new MetadataImage(
            new MetadataProvenance(200, 5, 4000),
            FeaturesImageTest.IMAGE2,
            ClusterImageTest.IMAGE2,
            TopicsImageTest.IMAGE2,
            ConfigurationsImageTest.IMAGE2,
            ClientQuotasImageTest.IMAGE2,
            ProducerIdsImageTest.IMAGE2,
            AclsImageTest.IMAGE2);
    }

    @Test
    public void testEmptyImageRoundTrip() throws Throwable {
        testToImageAndBack(MetadataImage.EMPTY);
    }

    @Test
    public void testImage1RoundTrip() throws Throwable {
        testToImageAndBack(IMAGE1);
    }

    @Test
    public void testApplyDelta1() throws Throwable {
        assertEquals(IMAGE2, DELTA1.apply(IMAGE2.provenance()));
    }

    @Test
    public void testImage2RoundTrip() throws Throwable {
        testToImageAndBack(IMAGE2);
    }

    private void testToImageAndBack(MetadataImage image) throws Throwable {
        RecordListWriter writer = new RecordListWriter();
        image.write(writer, new ImageWriterOptions.Builder(image).build());
        MetadataDelta delta = new MetadataDelta(MetadataImage.EMPTY);
        RecordTestUtils.replayAll(delta, writer.records());
        MetadataImage nextImage = delta.apply(image.provenance());
        assertEquals(image, nextImage);
    }
}
