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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.Optional;

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
            AclsImageTest.IMAGE1,
            ScramImageTest.IMAGE1,
            DelegationTokenImageTest.IMAGE1);

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
        RecordTestUtils.replayAll(DELTA1, ScramImageTest.DELTA1_RECORDS);
        RecordTestUtils.replayAll(DELTA1, DelegationTokenImageTest.DELTA1_RECORDS);

        IMAGE2 = new MetadataImage(
            new MetadataProvenance(200, 5, 4000),
            FeaturesImageTest.IMAGE2,
            ClusterImageTest.IMAGE2,
            TopicsImageTest.IMAGE2,
            ConfigurationsImageTest.IMAGE2,
            ClientQuotasImageTest.IMAGE2,
            ProducerIdsImageTest.IMAGE2,
            AclsImageTest.IMAGE2,
            ScramImageTest.IMAGE2,
            DelegationTokenImageTest.IMAGE2);
    }

    @Test
    public void testEmptyImageRoundTrip() {
        testToImage(MetadataImage.EMPTY);
    }

    @Test
    public void testImage1RoundTrip() {
        testToImage(IMAGE1);
    }

    @Test
    public void testApplyDelta1() {
        assertEquals(IMAGE2, DELTA1.apply(IMAGE2.provenance()));
        // check image1 + delta1 = image2, since records for image1 + delta1 might differ from records from image2
        ImageWriterOptions options = new ImageWriterOptions.Builder()
            .setMetadataVersion(IMAGE1.features().metadataVersion())
            .build();
        List<ApiMessageAndVersion> records = getImageRecords(IMAGE1, options);
        records.addAll(FeaturesImageTest.DELTA1_RECORDS);
        records.addAll(ClusterImageTest.DELTA1_RECORDS);
        records.addAll(TopicsImageTest.DELTA1_RECORDS);
        records.addAll(ConfigurationsImageTest.DELTA1_RECORDS);
        records.addAll(ClientQuotasImageTest.DELTA1_RECORDS);
        records.addAll(ProducerIdsImageTest.DELTA1_RECORDS);
        records.addAll(AclsImageTest.DELTA1_RECORDS);
        records.addAll(ScramImageTest.DELTA1_RECORDS);
        records.addAll(DelegationTokenImageTest.DELTA1_RECORDS);
        testToImage(IMAGE2, records);
    }

    @Test
    public void testImage2RoundTrip() {
        testToImage(IMAGE2);
    }

    private static void testToImage(MetadataImage image) {
        testToImage(image, new ImageWriterOptions.Builder()
            .setMetadataVersion(image.features().metadataVersion())
            .build(), Optional.empty());
    }

    static void testToImage(MetadataImage image, ImageWriterOptions options, Optional<List<ApiMessageAndVersion>> fromRecords) {
        testToImage(image, fromRecords.orElseGet(() -> getImageRecords(image, options)));
    }

    private static void testToImage(MetadataImage image, List<ApiMessageAndVersion> fromRecords) {
        // test from empty image stopping each of the various intermediate images along the way
        new RecordTestUtils.TestThroughAllIntermediateImagesLeadingToFinalImageHelper<MetadataDelta, MetadataImage>(
            () -> MetadataImage.EMPTY,
            MetadataDelta::new
        ) {
            @Override
            public MetadataImage createImageByApplyingDelta(MetadataDelta delta) {
                return delta.apply(image.provenance());
            }
        }.test(image, fromRecords);
    }

    private static List<ApiMessageAndVersion> getImageRecords(MetadataImage image, ImageWriterOptions options) {
        RecordListWriter writer = new RecordListWriter();
        image.write(writer, options);
        return writer.records();
    }
}
