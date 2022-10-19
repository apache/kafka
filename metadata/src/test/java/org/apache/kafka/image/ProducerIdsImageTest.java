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

import org.apache.kafka.common.metadata.ProducerIdsRecord;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.image.writer.RecordListWriter;
import org.apache.kafka.metadata.RecordTestUtils;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;


@Timeout(value = 40)
public class ProducerIdsImageTest {
    final static ProducerIdsImage IMAGE1;

    final static List<ApiMessageAndVersion> DELTA1_RECORDS;

    final static ProducerIdsDelta DELTA1;

    final static ProducerIdsImage IMAGE2;

    static {
        IMAGE1 = new ProducerIdsImage(123);

        DELTA1_RECORDS = new ArrayList<>();
        DELTA1_RECORDS.add(new ApiMessageAndVersion(new ProducerIdsRecord().
            setBrokerId(2).
            setBrokerEpoch(100).
            setNextProducerId(456), (short) 0));
        DELTA1_RECORDS.add(new ApiMessageAndVersion(new ProducerIdsRecord().
            setBrokerId(3).
            setBrokerEpoch(100).
            setNextProducerId(789), (short) 0));

        DELTA1 = new ProducerIdsDelta(IMAGE1);
        RecordTestUtils.replayAll(DELTA1, DELTA1_RECORDS);

        IMAGE2 = new ProducerIdsImage(789);
    }

    @Test
    public void testEmptyImageRoundTrip() throws Throwable {
        testToImageAndBack(ProducerIdsImage.EMPTY);
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

    private void testToImageAndBack(ProducerIdsImage image) throws Throwable {
        RecordListWriter writer = new RecordListWriter();
        image.write(writer, new ImageWriterOptions.Builder().build());
        ProducerIdsDelta delta = new ProducerIdsDelta(ProducerIdsImage.EMPTY);
        RecordTestUtils.replayAll(delta, writer.records());
        ProducerIdsImage nextImage = delta.apply();
        assertEquals(image, nextImage);
    }
}
