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

import org.apache.kafka.common.metadata.DelegationTokenRecord;
import org.apache.kafka.common.metadata.RemoveDelegationTokenRecord;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.token.delegation.TokenInformation;
import org.apache.kafka.common.utils.SecurityUtils;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.image.writer.RecordListWriter;
import org.apache.kafka.metadata.RecordTestUtils;
import org.apache.kafka.metadata.DelegationTokenData;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;


@Timeout(value = 40)
public class DelegationTokenImageTest {
    public final static DelegationTokenImage IMAGE1;

    public final static List<ApiMessageAndVersion> DELTA1_RECORDS;

    final static DelegationTokenDelta DELTA1;

    final static DelegationTokenImage IMAGE2;

    static DelegationTokenData randomDelegationTokenData(String tokenId, long expireTimestamp) {
        TokenInformation ti = new TokenInformation(
            tokenId,
            SecurityUtils.parseKafkaPrincipal(KafkaPrincipal.USER_TYPE + ":" + "fred"),
            SecurityUtils.parseKafkaPrincipal(KafkaPrincipal.USER_TYPE + ":" + "fred"),
            new ArrayList<KafkaPrincipal>(),
            0,
            1000,
            expireTimestamp);
        return new DelegationTokenData(ti);
    }

    static {
        Map<String, DelegationTokenData> image1 = new HashMap<>();
        image1.put("somerandomuuid1", randomDelegationTokenData("somerandomuuid1", 100));
        image1.put("somerandomuuid2", randomDelegationTokenData("somerandomuuid2", 100));
        image1.put("somerandomuuid3", randomDelegationTokenData("somerandomuuid3", 100));
        IMAGE1 = new DelegationTokenImage(image1);

        DELTA1_RECORDS = new ArrayList<>();
        DELTA1_RECORDS.add(new ApiMessageAndVersion(new DelegationTokenRecord().
            setOwner(KafkaPrincipal.USER_TYPE + ":" + "fred").
            setRequester(KafkaPrincipal.USER_TYPE + ":" + "fred").
            setIssueTimestamp(0).
            setMaxTimestamp(1000).
            setExpirationTimestamp(200).
            setTokenId("somerandomuuid1"), (short) 0));
        DELTA1_RECORDS.add(new ApiMessageAndVersion(new RemoveDelegationTokenRecord().
            setTokenId("somerandomuuid3"), (short) 0));

        DELTA1 = new DelegationTokenDelta(IMAGE1);
        RecordTestUtils.replayAll(DELTA1, DELTA1_RECORDS);

        Map<String, DelegationTokenData> image2 = new HashMap<>();
        image2.put("somerandomuuid1", randomDelegationTokenData("somerandomuuid1", 200));
        image2.put("somerandomuuid2", randomDelegationTokenData("somerandomuuid2", 100));
        IMAGE2 = new DelegationTokenImage(image2);
    }

    @Test
    public void testEmptyImageRoundTrip() throws Throwable {
        testToImage(DelegationTokenImage.EMPTY);
    }

    @Test
    public void testImage1RoundTrip() throws Throwable {
        testToImage(IMAGE1);
    }

    @Test
    public void testApplyDelta1() throws Throwable {
        assertEquals(IMAGE2, DELTA1.apply());
        // check image1 + delta1 = image2, since records for image1 + delta1 might differ from records from image2
        List<ApiMessageAndVersion> records = getImageRecords(IMAGE1);
        records.addAll(DELTA1_RECORDS);
        testToImage(IMAGE2, records);
    }

    @Test
    public void testImage2RoundTrip() throws Throwable {
        // testToImageAndBack(IMAGE2);
        testToImage(IMAGE2);
    }

    private static void testToImage(DelegationTokenImage image) {
        testToImage(image, Optional.empty());
    }

    private static void testToImage(DelegationTokenImage image, Optional<List<ApiMessageAndVersion>> fromRecords) {
        testToImage(image, fromRecords.orElseGet(() -> getImageRecords(image)));
    }

    private static void testToImage(DelegationTokenImage image, List<ApiMessageAndVersion> fromRecords) {
        // test from empty image stopping each of the various intermediate images along the way
        new RecordTestUtils.TestThroughAllIntermediateImagesLeadingToFinalImageHelper<>(
            () -> DelegationTokenImage.EMPTY,
            DelegationTokenDelta::new
        ).test(image, fromRecords);
    }

    private static List<ApiMessageAndVersion> getImageRecords(DelegationTokenImage image) {
        RecordListWriter writer = new RecordListWriter();
        image.write(writer, new ImageWriterOptions.Builder().build());
        return writer.records();
    }

    @Test
    public void testEmptyWithInvalidIBP() {
        ImageWriterOptions imageWriterOptions = new ImageWriterOptions.Builder().
                setMetadataVersion(MetadataVersion.IBP_3_5_IV2).build();
        RecordListWriter writer = new RecordListWriter();
        DelegationTokenImage.EMPTY.write(writer, imageWriterOptions);
    }

    @Test
    public void testImage1withInvalidIBP() {
        ImageWriterOptions imageWriterOptions = new ImageWriterOptions.Builder().
                setMetadataVersion(MetadataVersion.IBP_3_5_IV2).build();
        RecordListWriter writer = new RecordListWriter();
        try {
            IMAGE1.write(writer, imageWriterOptions);
            fail("expected exception writing IMAGE with Delegation Token records for MetadataVersion.IBP_3_5_IV2");
        } catch (Exception expected) {
            // ignore, expected
        }
    }
}
