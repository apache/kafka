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

import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.message.DescribeClientQuotasRequestData;
import org.apache.kafka.common.message.DescribeClientQuotasResponseData;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.image.writer.RecordListWriter;
import org.apache.kafka.metadata.RecordTestUtils;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.config.QuotaConfig;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.kafka.common.requests.DescribeClientQuotasRequest.MATCH_TYPE_DEFAULT;
import static org.apache.kafka.common.requests.DescribeClientQuotasRequest.MATCH_TYPE_EXACT;
import static org.apache.kafka.common.requests.DescribeClientQuotasRequest.MATCH_TYPE_SPECIFIED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Timeout(value = 40)
public class ClientQuotasImageTest {
    public static final ClientQuotasImage IMAGE1 = ClientQuotasImageFixtures.IMAGE1;

    public static final List<ApiMessageAndVersion> DELTA1_RECORDS = ClientQuotasImageFixtures.DELTA1_RECORDS;

    static final ClientQuotasDelta DELTA1 = ClientQuotasImageFixtures.DELTA1;

    static final ClientQuotasImage IMAGE2 = ClientQuotasImageFixtures.IMAGE2;

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

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {ClientQuotaEntity.USER, ClientQuotaEntity.CLIENT_ID, ClientQuotaEntity.IP})
    public void testDescribeWithNonStrictExactMatch(String entityType) {
        Map<ClientQuotaEntity, ClientQuotaImage> entities = Map.of(
            new ClientQuotaEntity(Map.of(ClientQuotaEntity.USER, "foo", ClientQuotaEntity.CLIENT_ID, "baz")), new ClientQuotaImage(Map.of(QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, 100.0)),
            new ClientQuotaEntity(Map.of(ClientQuotaEntity.USER, "bar", ClientQuotaEntity.CLIENT_ID, "baz")), new ClientQuotaImage(Map.of(QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, 200.0)),
            new ClientQuotaEntity(Map.of(ClientQuotaEntity.CLIENT_ID, "foo", ClientQuotaEntity.USER, "baz")), new ClientQuotaImage(Map.of(QuotaConfig.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG, 100.0)),
            new ClientQuotaEntity(Map.of(ClientQuotaEntity.CLIENT_ID, "bar", ClientQuotaEntity.USER, "baz")), new ClientQuotaImage(Map.of(QuotaConfig.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG, 200.0)),
            new ClientQuotaEntity(Map.of(ClientQuotaEntity.IP, "foo", ClientQuotaEntity.USER, "baz")), new ClientQuotaImage(Map.of(QuotaConfig.REQUEST_PERCENTAGE_OVERRIDE_CONFIG, 10.0)),
            new ClientQuotaEntity(Map.of(ClientQuotaEntity.IP, "bar", ClientQuotaEntity.USER, "baz")), new ClientQuotaImage(Map.of(QuotaConfig.REQUEST_PERCENTAGE_OVERRIDE_CONFIG, 20.0))
        );
        ClientQuotasImage image = new ClientQuotasImage(entities);

        DescribeClientQuotasRequestData request = new DescribeClientQuotasRequestData()
            .setComponents(List.of(
                new DescribeClientQuotasRequestData.ComponentData()
                    .setEntityType(entityType)
                    .setMatchType(MATCH_TYPE_EXACT)
                    .setMatch("foo")));

        DescribeClientQuotasResponseData response = image.describe(request);
        assertEquals(1, response.entries().size());
        Optional<DescribeClientQuotasResponseData.EntityData> entity = response.entries().get(0).entity().stream()
            .filter(e -> e.entityType().equals(entityType))
            .findFirst();
        assertTrue(entity.isPresent());
        assertEquals("foo", entity.get().entityName());

        request = new DescribeClientQuotasRequestData()
            .setComponents(List.of(
                new DescribeClientQuotasRequestData.ComponentData()
                    .setEntityType(entityType)
                    .setMatchType(MATCH_TYPE_EXACT)
                    .setMatch("nonexistent")));
        response = image.describe(request);
        assertEquals(0, response.entries().size());
    }

    @Test
    public void testDescribeWithStrictMode() {
        Map<ClientQuotaEntity, ClientQuotaImage> entities = Map.of(
            new ClientQuotaEntity(Map.of(ClientQuotaEntity.USER, "foo", ClientQuotaEntity.CLIENT_ID, "id1")), new ClientQuotaImage(Map.of(QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, 100.0)),
            new ClientQuotaEntity(Map.of(ClientQuotaEntity.USER, "foo", ClientQuotaEntity.CLIENT_ID, "id1", ClientQuotaEntity.IP, "ip")), new ClientQuotaImage(Map.of(QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, 200.0)),
            new ClientQuotaEntity(Map.of(ClientQuotaEntity.USER, "bar", ClientQuotaEntity.CLIENT_ID, "id2")), new ClientQuotaImage(Map.of(QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, 300.0))
        );
        ClientQuotasImage image = new ClientQuotasImage(entities);

        // 1. All exact match
        DescribeClientQuotasRequestData allExactMatchRequest = new DescribeClientQuotasRequestData()
            .setStrict(true)
            .setComponents(List.of(
                new DescribeClientQuotasRequestData.ComponentData()
                    .setEntityType(ClientQuotaEntity.USER)
                    .setMatchType(MATCH_TYPE_EXACT)
                    .setMatch("foo"),
                new DescribeClientQuotasRequestData.ComponentData()
                    .setEntityType(ClientQuotaEntity.CLIENT_ID)
                    .setMatchType(MATCH_TYPE_EXACT)
                    .setMatch("id1")
            ));
        DescribeClientQuotasResponseData allExactMatchResponse = image.describe(allExactMatchRequest);
        assertEquals(1, allExactMatchResponse.entries().size());
        List<DescribeClientQuotasResponseData.EntityData> allExactMatchEntities = allExactMatchResponse.entries().get(0).entity();
        assertTrue(allExactMatchEntities.contains(new DescribeClientQuotasResponseData.EntityData()
            .setEntityType(ClientQuotaEntity.CLIENT_ID)
            .setEntityName("id1")));
        assertTrue(allExactMatchEntities.contains(new DescribeClientQuotasResponseData.EntityData()
            .setEntityType(ClientQuotaEntity.USER)
            .setEntityName("foo")));

        // 2. All match type specified
        DescribeClientQuotasRequestData allTypeMatchRequest = new DescribeClientQuotasRequestData()
            .setStrict(true)
            .setComponents(List.of(
                new DescribeClientQuotasRequestData.ComponentData()
                    .setEntityType(ClientQuotaEntity.USER)
                    .setMatchType(MATCH_TYPE_SPECIFIED)
                    .setMatch(null),
                new DescribeClientQuotasRequestData.ComponentData()
                    .setEntityType(ClientQuotaEntity.CLIENT_ID)
                    .setMatchType(MATCH_TYPE_SPECIFIED)
                    .setMatch(null)
            ));
        DescribeClientQuotasResponseData allTypeMatchResponse = image.describe(allTypeMatchRequest);
        assertEquals(2, allTypeMatchResponse.entries().size());
        for (DescribeClientQuotasResponseData.EntryData entry : allTypeMatchResponse.entries()) {
            for  (DescribeClientQuotasResponseData.EntityData entity : entry.entity()) {
                assertNotEquals(ClientQuotaEntity.IP, entity.entityType());
            }
        }

        // 3. Mixed exact and match type specified
        DescribeClientQuotasRequestData exactAndMatchTypeRequest = new DescribeClientQuotasRequestData()
            .setStrict(true)
            .setComponents(List.of(
                new DescribeClientQuotasRequestData.ComponentData()
                    .setEntityType(ClientQuotaEntity.USER)
                    .setMatchType(MATCH_TYPE_EXACT)
                    .setMatch("foo"),
                new DescribeClientQuotasRequestData.ComponentData()
                    .setEntityType(ClientQuotaEntity.CLIENT_ID)
                    .setMatchType(MATCH_TYPE_SPECIFIED)
                    .setMatch(null)
            ));
        DescribeClientQuotasResponseData exactAndMatchTypeResponse = image.describe(exactAndMatchTypeRequest);
        assertEquals(1, exactAndMatchTypeResponse.entries().size());
        List<DescribeClientQuotasResponseData.EntityData> exactAndMatchEntities = allExactMatchResponse.entries().get(0).entity();
        assertTrue(exactAndMatchEntities.contains(new DescribeClientQuotasResponseData.EntityData()
            .setEntityType(ClientQuotaEntity.CLIENT_ID)
            .setEntityName("id1")));
        assertTrue(exactAndMatchEntities.contains(new DescribeClientQuotasResponseData.EntityData()
            .setEntityType(ClientQuotaEntity.USER)
            .setEntityName("foo")));
    }

    @Test
    public void testDescribeWithNonStrictTypeMatch() {
        Map<ClientQuotaEntity, ClientQuotaImage> entities = Map.of(
            new ClientQuotaEntity(Map.of(ClientQuotaEntity.USER, "foo", ClientQuotaEntity.CLIENT_ID, "id")), new ClientQuotaImage(Map.of(QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, 100.0)),
            new ClientQuotaEntity(Map.of(ClientQuotaEntity.USER, "bar", ClientQuotaEntity.IP, "ip")), new ClientQuotaImage(Map.of(QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, 200.0)),
            new ClientQuotaEntity(Map.of(ClientQuotaEntity.IP, "ip", ClientQuotaEntity.CLIENT_ID, "id")), new ClientQuotaImage(Map.of(QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, 200.0))
        );
        ClientQuotasImage image = new ClientQuotasImage(entities);
        DescribeClientQuotasRequestData request = new DescribeClientQuotasRequestData()
            .setComponents(List.of(
                new DescribeClientQuotasRequestData.ComponentData()
                    .setEntityType(ClientQuotaEntity.USER)
                    .setMatchType(MATCH_TYPE_SPECIFIED)
                    .setMatch(null)
            ));

        DescribeClientQuotasResponseData response = image.describe(request);
        assertEquals(2, response.entries().size());
        for (DescribeClientQuotasResponseData.EntryData entry : response.entries()) {
            assertTrue(entry.entity().stream().anyMatch(e -> e.entityType().equals(ClientQuotaEntity.USER)));
        }
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {ClientQuotaEntity.USER, ClientQuotaEntity.CLIENT_ID, ClientQuotaEntity.IP})
    public void testDescribeWithNonStrictDefaultMatch(String entityType) {
        Map<String, String> defaultEntity = new HashMap<>();
        defaultEntity.put(entityType, null);
        Map<ClientQuotaEntity, ClientQuotaImage> entities = Map.of(
            new ClientQuotaEntity(defaultEntity), new ClientQuotaImage(Map.of(QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, 100.0)),
            new ClientQuotaEntity(Map.of(entityType, "foo")), new ClientQuotaImage(Map.of(QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, 200.0))
        );
        ClientQuotasImage image = new ClientQuotasImage(entities);

        DescribeClientQuotasRequestData request = new DescribeClientQuotasRequestData()
            .setComponents(List.of(
                new DescribeClientQuotasRequestData.ComponentData()
                    .setEntityType(entityType)
                    .setMatchType(MATCH_TYPE_DEFAULT)
                    .setMatch(null)
            ));

        DescribeClientQuotasResponseData response = image.describe(request);
        assertEquals(1, response.entries().size());
        assertEquals(entityType, response.entries().get(0).entity().get(0).entityType());
        assertNull(response.entries().get(0).entity().get(0).entityName());
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {ClientQuotaEntity.USER, ClientQuotaEntity.CLIENT_ID, ClientQuotaEntity.IP})
    public void testDescribeDefaultMatchWithNoData(String entityType) {
        ClientQuotasImage image = new ClientQuotasImage(Map.of());

        DescribeClientQuotasRequestData request = new DescribeClientQuotasRequestData()
            .setComponents(List.of(
                new DescribeClientQuotasRequestData.ComponentData()
                    .setEntityType(entityType)
                    .setMatchType(MATCH_TYPE_DEFAULT)
                    .setMatch(null)
            ));

        DescribeClientQuotasResponseData response = image.describe(request);
        assertEquals(0, response.entries().size());
    }

    @Test
    public void testDescribeNonStrictEmptyRequest() {
        Map<ClientQuotaEntity, ClientQuotaImage> entities = Map.of(
            new ClientQuotaEntity(Map.of(ClientQuotaEntity.USER, "foo")), new ClientQuotaImage(Map.of(QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, 100.0)),
            new ClientQuotaEntity(Map.of(ClientQuotaEntity.CLIENT_ID, "bar")), new ClientQuotaImage(Map.of(QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, 200.0)),
            new ClientQuotaEntity(Map.of(ClientQuotaEntity.IP, "baz")), new ClientQuotaImage(Map.of(QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, 300.0))
        );
        ClientQuotasImage image = new ClientQuotasImage(entities);
        DescribeClientQuotasRequestData request = new DescribeClientQuotasRequestData()
            .setStrict(false)
            .setComponents(List.of());

        DescribeClientQuotasResponseData response = image.describe(request);
        assertEquals(3, response.entries().size());
    }

    @Test
    public void testDescribeWithEmptyEntityType() {
        DescribeClientQuotasRequestData request = new DescribeClientQuotasRequestData()
            .setComponents(List.of(
                new DescribeClientQuotasRequestData.ComponentData()
                    .setEntityType("")
                    .setMatchType(MATCH_TYPE_EXACT)
                    .setMatch("foo")));

        InvalidRequestException exception = assertThrows(InvalidRequestException.class, () -> IMAGE1.describe(request));
        assertEquals("Invalid empty entity type.", exception.getMessage());
    }

    @Test
    public void testDescribeWithDuplicateEntityType() {
        DescribeClientQuotasRequestData request = new DescribeClientQuotasRequestData()
            .setComponents(List.of(
                new DescribeClientQuotasRequestData.ComponentData()
                    .setEntityType(ClientQuotaEntity.USER)
                    .setMatchType(MATCH_TYPE_EXACT)
                    .setMatch("foo"),
                new DescribeClientQuotasRequestData.ComponentData()
                    .setEntityType(ClientQuotaEntity.USER)
                    .setMatchType(MATCH_TYPE_SPECIFIED)));

        InvalidRequestException exception = assertThrows(InvalidRequestException.class, () -> IMAGE1.describe(request));
        assertEquals("Entity type user cannot appear more than once in the filter.", exception.getMessage());
    }

    @Test
    public void testDescribeWithExactMatchNullMatch() {
        DescribeClientQuotasRequestData request = new DescribeClientQuotasRequestData()
            .setComponents(List.of(
                new DescribeClientQuotasRequestData.ComponentData()
                    .setEntityType(ClientQuotaEntity.USER)
                    .setMatchType(MATCH_TYPE_EXACT)
                    .setMatch(null)));

        InvalidRequestException exception = assertThrows(InvalidRequestException.class, () -> IMAGE1.describe(request));
        assertEquals("Request specified MATCH_TYPE_EXACT, but set match string to null.", exception.getMessage());
    }

    @Test
    public void testDescribeWithDefaultMatchNonNullMatch() {
        DescribeClientQuotasRequestData request = new DescribeClientQuotasRequestData()
            .setComponents(List.of(
                new DescribeClientQuotasRequestData.ComponentData()
                    .setEntityType(ClientQuotaEntity.USER)
                    .setMatchType(MATCH_TYPE_DEFAULT)
                    .setMatch("foo")));

        InvalidRequestException exception = assertThrows(InvalidRequestException.class, () -> IMAGE1.describe(request));
        assertEquals("Request specified MATCH_TYPE_DEFAULT, but also specified a match string.", exception.getMessage());
    }

    @Test
    public void testDescribeWithSpecifiedMatchNonNullMatch() {
        DescribeClientQuotasRequestData request = new DescribeClientQuotasRequestData()
            .setComponents(List.of(
                new DescribeClientQuotasRequestData.ComponentData()
                    .setEntityType(ClientQuotaEntity.USER)
                    .setMatchType(MATCH_TYPE_SPECIFIED)
                    .setMatch("foo")));

        InvalidRequestException exception = assertThrows(InvalidRequestException.class, () -> IMAGE1.describe(request));
        assertEquals("Request specified MATCH_TYPE_SPECIFIED, but also specified a match string.", exception.getMessage());
    }

    @Test
    public void testDescribeWithUnknownMatchType() {
        DescribeClientQuotasRequestData request = new DescribeClientQuotasRequestData()
            .setComponents(List.of(
                new DescribeClientQuotasRequestData.ComponentData()
                    .setEntityType(ClientQuotaEntity.USER)
                    .setMatchType((byte) 99)));

        InvalidRequestException exception = assertThrows(InvalidRequestException.class, () -> IMAGE1.describe(request));
        assertEquals("Unknown match type 99", exception.getMessage());
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {ClientQuotaEntity.USER, ClientQuotaEntity.CLIENT_ID})
    public void testDescribeWithIpAndOtherTypeCombination(String entityType) {
        DescribeClientQuotasRequestData request = new DescribeClientQuotasRequestData()
            .setComponents(List.of(
                new DescribeClientQuotasRequestData.ComponentData()
                    .setEntityType(ClientQuotaEntity.IP)
                    .setMatchType(MATCH_TYPE_EXACT)
                    .setMatch("127.0.0.1"),
                new DescribeClientQuotasRequestData.ComponentData()
                    .setEntityType(entityType)
                    .setMatchType(MATCH_TYPE_EXACT)
                    .setMatch("foo")));

        InvalidRequestException exception = assertThrows(InvalidRequestException.class, () -> IMAGE1.describe(request));
        assertTrue(exception.getMessage().contains("IP filter component should not be used with user or clientId filter component"));
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
}
