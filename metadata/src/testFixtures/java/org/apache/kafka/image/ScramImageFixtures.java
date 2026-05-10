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
import org.apache.kafka.metadata.RecordTestUtils;
import org.apache.kafka.metadata.ScramCredentialData;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.util.MockRandom;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.apache.kafka.clients.admin.ScramMechanism.SCRAM_SHA_256;
import static org.apache.kafka.clients.admin.ScramMechanism.SCRAM_SHA_512;

public final class ScramImageFixtures {
    public static final ScramImage IMAGE1;
    public static final List<ApiMessageAndVersion> DELTA1_RECORDS;
    public static final ScramDelta DELTA1;
    public static final ScramImage IMAGE2;

    private static byte[] randomBuffer(Random random, int length) {
        byte[] buf = new byte[length];
        random.nextBytes(buf);
        return buf;
    }

    private static ScramCredentialData randomScramCredentialData(Random random) {
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

    private ScramImageFixtures() {
    }
}
