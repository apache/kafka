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
import org.apache.kafka.server.common.MetadataVersion;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;


@Timeout(value = 40)
public class ScramImageTest {
    public static final ScramImage IMAGE1 = ScramImageFixtures.IMAGE1;

    public static final List<ApiMessageAndVersion> DELTA1_RECORDS = ScramImageFixtures.DELTA1_RECORDS;

    static final ScramDelta DELTA1 = ScramImageFixtures.DELTA1;

    static final ScramImage IMAGE2 = ScramImageFixtures.IMAGE2;

    @Test
    public void testEmptyImageRoundTrip() {
        testToImage(ScramImage.EMPTY);
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

    private static void testToImage(ScramImage image) {
        testToImage(image, Optional.empty());
    }

    private static void testToImage(ScramImage image, Optional<List<ApiMessageAndVersion>> fromRecords) {
        testToImage(image, fromRecords.orElseGet(() -> getImageRecords(image)));
    }

    private static void testToImage(ScramImage image, List<ApiMessageAndVersion> fromRecords) {
        // test from empty image stopping each of the various intermediate images along the way
        new RecordTestUtils.TestThroughAllIntermediateImagesLeadingToFinalImageHelper<>(
            () -> ScramImage.EMPTY,
            ScramDelta::new
        ).test(image, fromRecords);
    }

    private static List<ApiMessageAndVersion> getImageRecords(ScramImage image) {
        RecordListWriter writer = new RecordListWriter();
        image.write(writer, new ImageWriterOptions.Builder(MetadataVersion.latestProduction()).build());
        return writer.records();
    }

    @Test
    public void testEmptyWithInvalidIBP() {
        ImageWriterOptions imageWriterOptions = new ImageWriterOptions.Builder(MetadataVersion.IBP_3_4_IV0).build();
        RecordListWriter writer = new RecordListWriter();
        ScramImage.EMPTY.write(writer, imageWriterOptions);
    }

    @Test
    public void testImage1withInvalidIBP() {
        ImageWriterOptions imageWriterOptions = new ImageWriterOptions.Builder(MetadataVersion.IBP_3_4_IV0).build();
        RecordListWriter writer = new RecordListWriter();
        assertThrows(Exception.class, () -> IMAGE1.write(writer, imageWriterOptions),
            "expected exception writing IMAGE with SCRAM records for MetadataVersion.IBP_3_4_IV0");
    }
}
