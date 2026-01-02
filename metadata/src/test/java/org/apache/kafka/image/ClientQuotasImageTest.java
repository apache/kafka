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

import org.apache.kafka.common.metadata.ClientQuotaRecord;
import org.apache.kafka.common.metadata.ClientQuotaRecord.EntityData;
import org.apache.kafka.common.message.DescribeClientQuotasRequestData;
import org.apache.kafka.common.message.DescribeClientQuotasResponseData;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.image.writer.RecordListWriter;
import org.apache.kafka.metadata.RecordTestUtils;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.config.QuotaConfig;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.kafka.common.metadata.MetadataRecordType.CLIENT_QUOTA_RECORD;
import static org.apache.kafka.common.requests.DescribeClientQuotasRequest.MATCH_TYPE_EXACT;
import static org.apache.kafka.common.requests.DescribeClientQuotasRequest.MATCH_TYPE_SPECIFIED;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Timeout(value = 40)
public class ClientQuotasImageTest {
    public static final ClientQuotasImage IMAGE1;

    public static final List<ApiMessageAndVersion> DELTA1_RECORDS;

    static final ClientQuotasDelta DELTA1;

    static final ClientQuotasImage IMAGE2;

    static {
        Map<ClientQuotaEntity, ClientQuotaImage> entities1 = new HashMap<>();
        Map<String, String> fooUser = Map.of(ClientQuotaEntity.USER, "foo");
        Map<String, Double> fooUserQuotas = Map.of(QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, 123.0);
        entities1.put(new ClientQuotaEntity(fooUser), new ClientQuotaImage(fooUserQuotas));
        Map<String, String> barUserAndIp = new HashMap<>();
        barUserAndIp.put(ClientQuotaEntity.USER, "bar");
        barUserAndIp.put(ClientQuotaEntity.IP, "127.0.0.1");
        Map<String, Double> barUserAndIpQuotas = Map.of(QuotaConfig.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG, 456.0);
        entities1.put(new ClientQuotaEntity(barUserAndIp), new ClientQuotaImage(barUserAndIpQuotas));
        IMAGE1 = new ClientQuotasImage(entities1);

        DELTA1_RECORDS = new ArrayList<>();
        // remove quota
        DELTA1_RECORDS.add(new ApiMessageAndVersion(new ClientQuotaRecord().setEntity(List.of(
                new EntityData().setEntityType(ClientQuotaEntity.USER).setEntityName("bar"),
                new EntityData().setEntityType(ClientQuotaEntity.IP).setEntityName("127.0.0.1")))
                .setKey(QuotaConfig.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG).setRemove(true),
                CLIENT_QUOTA_RECORD.highestSupportedVersion()));
        // alter quota
        DELTA1_RECORDS.add(new ApiMessageAndVersion(new ClientQuotaRecord().setEntity(List.of(
                new EntityData().setEntityType(ClientQuotaEntity.USER).setEntityName("foo")))
                .setKey(QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG).setValue(234.0),
                CLIENT_QUOTA_RECORD.highestSupportedVersion()));
        // add quota to entity with existing quota
        DELTA1_RECORDS.add(new ApiMessageAndVersion(new ClientQuotaRecord().setEntity(List.of(
                new EntityData().setEntityType(ClientQuotaEntity.USER).setEntityName("foo")))
                .setKey(QuotaConfig.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG).setValue(999.0),
                CLIENT_QUOTA_RECORD.highestSupportedVersion()));

        DELTA1 = new ClientQuotasDelta(IMAGE1);
        RecordTestUtils.replayAll(DELTA1, DELTA1_RECORDS);

        Map<ClientQuotaEntity, ClientQuotaImage> entities2 = new HashMap<>();
        Map<String, Double> fooUserQuotas2 = new HashMap<>();
        fooUserQuotas2.put(QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, 234.0);
        fooUserQuotas2.put(QuotaConfig.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG, 999.0);
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
        image.write(writer);
        return writer.records();
    }
    @Test
    public void testDescribeMatches() {
        Map<ClientQuotaEntity, ClientQuotaImage> entities = new HashMap<>();
        Map<String, String> user1 = Map.of(ClientQuotaEntity.USER, "user1");
        entities.put(new ClientQuotaEntity(user1), new ClientQuotaImage(Map.of("k1", 1.0)));

        Map<String, String> user2 = Map.of(ClientQuotaEntity.USER, "user2");
        entities.put(new ClientQuotaEntity(user2), new ClientQuotaImage(Map.of("k1", 2.0)));

        Map<String, String> client1 = Map.of(ClientQuotaEntity.CLIENT_ID, "client1");
        entities.put(new ClientQuotaEntity(client1), new ClientQuotaImage(Map.of("k2", 3.0)));

        ClientQuotasImage image = new ClientQuotasImage(entities);

        // Test exact match
        DescribeClientQuotasRequestData request1 = new DescribeClientQuotasRequestData();
        request1.components().add(new DescribeClientQuotasRequestData.ComponentData().
            setEntityType(ClientQuotaEntity.USER).setMatchType(MATCH_TYPE_EXACT).setMatch("user1"));
        DescribeClientQuotasResponseData response1 = image.describe(request1);
        assertEquals(1, response1.entries().size());
        assertEquals("user1", response1.entries().get(0).entity().get(0).entityName());

        // Test type match
        DescribeClientQuotasRequestData request2 = new DescribeClientQuotasRequestData();
        request2.components().add(new DescribeClientQuotasRequestData.ComponentData().
            setEntityType(ClientQuotaEntity.USER).setMatchType(MATCH_TYPE_SPECIFIED).setMatch(null));
        DescribeClientQuotasResponseData response2 = image.describe(request2);
        assertEquals(2, response2.entries().size());

        // Test no match
        DescribeClientQuotasRequestData request3 = new DescribeClientQuotasRequestData();
        request3.components().add(new DescribeClientQuotasRequestData.ComponentData().
            setEntityType(ClientQuotaEntity.USER).setMatchType(MATCH_TYPE_EXACT).setMatch("unknown"));
        DescribeClientQuotasResponseData response3 = image.describe(request3);
        assertEquals(0, response3.entries().size());
    }
}
