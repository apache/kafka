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

import org.apache.kafka.common.config.internals.QuotaConfigs;
import org.apache.kafka.common.metadata.ClientQuotaRecord;
import org.apache.kafka.common.metadata.ClientQuotaRecord.EntityData;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.image.writer.RecordListWriter;
import org.apache.kafka.metadata.RecordTestUtils;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.kafka.common.metadata.MetadataRecordType.CLIENT_QUOTA_RECORD;
import static org.junit.jupiter.api.Assertions.assertEquals;


@Timeout(value = 40)
public class ClientQuotasImageTest {
    public final static ClientQuotasImage IMAGE1;

    public final static List<ApiMessageAndVersion> DELTA1_RECORDS;

    final static ClientQuotasDelta DELTA1;

    final static ClientQuotasImage IMAGE2;

    static {
        Map<ClientQuotaEntity, ClientQuotaImage> entities1 = new HashMap<>();
        Map<String, String> fooUser = new HashMap<>();
        fooUser.put(ClientQuotaEntity.USER, "foo");
        Map<String, Double> fooUserQuotas = new HashMap<>();
        fooUserQuotas.put(QuotaConfigs.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, 123.0);
        entities1.put(new ClientQuotaEntity(fooUser), new ClientQuotaImage(fooUserQuotas));
        Map<String, String> barUserAndIp = new HashMap<>();
        barUserAndIp.put(ClientQuotaEntity.USER, "bar");
        barUserAndIp.put(ClientQuotaEntity.IP, "127.0.0.1");
        Map<String, Double> barUserAndIpQuotas = new HashMap<>();
        barUserAndIpQuotas.put(QuotaConfigs.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG, 456.0);
        entities1.put(new ClientQuotaEntity(barUserAndIp),
            new ClientQuotaImage(barUserAndIpQuotas));
        IMAGE1 = new ClientQuotasImage(entities1);

        DELTA1_RECORDS = new ArrayList<>();
        DELTA1_RECORDS.add(new ApiMessageAndVersion(new ClientQuotaRecord().
                setEntity(Arrays.asList(
                    new EntityData().setEntityType(ClientQuotaEntity.USER).setEntityName("bar"),
                    new EntityData().setEntityType(ClientQuotaEntity.IP).setEntityName("127.0.0.1"))).
                setKey(QuotaConfigs.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG).
                setRemove(true), CLIENT_QUOTA_RECORD.highestSupportedVersion()));
        DELTA1_RECORDS.add(new ApiMessageAndVersion(new ClientQuotaRecord().
            setEntity(Arrays.asList(
                new EntityData().setEntityType(ClientQuotaEntity.USER).setEntityName("foo"))).
            setKey(QuotaConfigs.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG).
            setValue(999.0), CLIENT_QUOTA_RECORD.highestSupportedVersion()));

        DELTA1 = new ClientQuotasDelta(IMAGE1);
        RecordTestUtils.replayAll(DELTA1, DELTA1_RECORDS);

        Map<ClientQuotaEntity, ClientQuotaImage> entities2 = new HashMap<>();
        Map<String, Double> fooUserQuotas2 = new HashMap<>();
        fooUserQuotas2.put(QuotaConfigs.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, 123.0);
        fooUserQuotas2.put(QuotaConfigs.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG, 999.0);
        entities2.put(new ClientQuotaEntity(fooUser), new ClientQuotaImage(fooUserQuotas2));
        IMAGE2 = new ClientQuotasImage(entities2);
    }

    @Test
    public void testEmptyImageRoundTrip() {
        testToImage(ClientQuotasImage.EMPTY);
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

    private static void testToImage(ClientQuotasImage image) {
        testToImage(image, Optional.empty());
    }

    private static void testToImage(ClientQuotasImage image, Optional<List<ApiMessageAndVersion>> fromRecords) {
        testToImage(image, fromRecords.orElseGet(() -> getImageRecords(image)));
    }

    private static void testToImage(ClientQuotasImage image, List<ApiMessageAndVersion> fromRecords) {
        // test from empty image stopping each of the various intermediate images along the way
        new RecordTestUtils.TestThroughAllIntermediateImagesLeadingToFinalImageHelper<>(
            () -> ClientQuotasImage.EMPTY,
            ClientQuotasDelta::new
        ).test(image, fromRecords);
    }

    private static List<ApiMessageAndVersion> getImageRecords(ClientQuotasImage image) {
        RecordListWriter writer = new RecordListWriter();
        image.write(writer, new ImageWriterOptions.Builder().build());
        return writer.records();
    }
}
