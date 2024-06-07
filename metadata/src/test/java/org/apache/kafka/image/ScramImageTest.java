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

import org.apache.kafka.clients.admin.ScramMechanism;
import org.apache.kafka.common.metadata.RemoveUserScramCredentialRecord;
import org.apache.kafka.common.metadata.UserScramCredentialRecord;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.image.writer.RecordListWriter;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.util.MockRandom;
import org.apache.kafka.metadata.RecordTestUtils;
import org.apache.kafka.metadata.ScramCredentialData;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

import static org.apache.kafka.clients.admin.ScramMechanism.SCRAM_SHA_256;
import static org.apache.kafka.clients.admin.ScramMechanism.SCRAM_SHA_512;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;


@Timeout(value = 40)
public class ScramImageTest {
    public final static ScramImage IMAGE1;

    public final static List<ApiMessageAndVersion> DELTA1_RECORDS;

    final static ScramDelta DELTA1;

    final static ScramImage IMAGE2;

    static byte[] randomBuffer(Random random, int length) {
        byte[] buf = new byte[length];
        random.nextBytes(buf);
        return buf;
    }

    static ScramCredentialData randomScramCredentialData(Random random) {
        return new ScramCredentialData(
            randomBuffer(random, 1024),
            randomBuffer(random, 1024),
            randomBuffer(random, 1024),
            1024 + random.nextInt(1024));
    }

    static {
        MockRandom random = new MockRandom();

        Map<ScramMechanism, Map<String, ScramCredentialData>> image1mechanisms = new HashMap<>();

        Map<String, ScramCredentialData> image1sha256 = new HashMap<>();
        image1sha256.put("alpha", randomScramCredentialData(random));
        image1sha256.put("beta", randomScramCredentialData(random));
        image1mechanisms.put(SCRAM_SHA_256, image1sha256);

        Map<String, ScramCredentialData> image1sha512 = new HashMap<>();
        image1sha512.put("alpha", randomScramCredentialData(random));
        image1sha512.put("gamma", randomScramCredentialData(random));
        image1mechanisms.put(SCRAM_SHA_512, image1sha512);

        IMAGE1 = new ScramImage(image1mechanisms);

        DELTA1_RECORDS = new ArrayList<>();
        // remove all sha512 credentials
        DELTA1_RECORDS.add(new ApiMessageAndVersion(new RemoveUserScramCredentialRecord().
            setName("alpha").
            setMechanism(SCRAM_SHA_512.type()), (short) 0));
        DELTA1_RECORDS.add(new ApiMessageAndVersion(new RemoveUserScramCredentialRecord().
            setName("gamma").
            setMechanism(SCRAM_SHA_512.type()), (short) 0));
        ScramCredentialData secondAlpha256Credential = randomScramCredentialData(random);
        // add sha256 credential
        DELTA1_RECORDS.add(new ApiMessageAndVersion(new UserScramCredentialRecord().
                setName("alpha").
                setMechanism(SCRAM_SHA_256.type()).
                setSalt(secondAlpha256Credential.salt()).
                setStoredKey(secondAlpha256Credential.storedKey()).
                setServerKey(secondAlpha256Credential.serverKey()).
                setIterations(secondAlpha256Credential.iterations()), (short) 0));
        // add sha512 credential re-using name
        ScramCredentialData secondAlpha512Credential = randomScramCredentialData(random);
        DELTA1_RECORDS.add(new ApiMessageAndVersion(new UserScramCredentialRecord().
            setName("alpha").
            setMechanism(SCRAM_SHA_512.type()).
            setSalt(secondAlpha512Credential.salt()).
            setStoredKey(secondAlpha512Credential.storedKey()).
            setServerKey(secondAlpha512Credential.serverKey()).
            setIterations(secondAlpha512Credential.iterations()), (short) 0));
        DELTA1 = new ScramDelta(IMAGE1);
        RecordTestUtils.replayAll(DELTA1, DELTA1_RECORDS);

        Map<ScramMechanism, Map<String, ScramCredentialData>> image2mechanisms = new HashMap<>();

        Map<String, ScramCredentialData> image2sha256 = new HashMap<>();
        image2sha256.put("alpha", secondAlpha256Credential);
        image2sha256.put("beta", image1sha256.get("beta"));
        image2mechanisms.put(SCRAM_SHA_256, image2sha256);

        Map<String, ScramCredentialData> image2sha512 = new HashMap<>();
        image2sha512.put("alpha", secondAlpha512Credential);
        image2mechanisms.put(SCRAM_SHA_512, image2sha512);

        IMAGE2 = new ScramImage(image2mechanisms);
    }

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
        image.write(writer, new ImageWriterOptions.Builder().build());
        return writer.records();
    }

    @Test
    public void testEmptyWithInvalidIBP() {
        ImageWriterOptions imageWriterOptions = new ImageWriterOptions.Builder().
                setMetadataVersion(MetadataVersion.IBP_3_4_IV0).build();
        RecordListWriter writer = new RecordListWriter();
        ScramImage.EMPTY.write(writer, imageWriterOptions);
    }

    @Test
    public void testImage1withInvalidIBP() {
        ImageWriterOptions imageWriterOptions = new ImageWriterOptions.Builder().
                setMetadataVersion(MetadataVersion.IBP_3_4_IV0).build();
        RecordListWriter writer = new RecordListWriter();
        try {
            IMAGE1.write(writer, imageWriterOptions);
            fail("expected exception writing IMAGE with SCRAM records for MetadataVersion.IBP_3_4_IV0");
        } catch (Exception expected) {
            // ignore, expected
        }
    }
}
