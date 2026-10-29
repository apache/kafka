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

import org.apache.kafka.common.metadata.RemoveUserScramCredentialRecord;
import org.apache.kafka.common.metadata.UserScramCredentialRecord;
import org.apache.kafka.metadata.RecordTestUtils;
import org.apache.kafka.metadata.ScramCredentialData;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.util.MockRandom;

import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.apache.kafka.clients.admin.ScramMechanism.SCRAM_SHA_256;
import static org.apache.kafka.clients.admin.ScramMechanism.SCRAM_SHA_512;

public final class ScramImageFixtures {

    private static final MockRandom RANDOM = new MockRandom();

    private static final ScramCredentialData ALPHA_SHA_256_FIRST = randomScramCredentialData(RANDOM);
    private static final ScramCredentialData BETA_SHA_256 = randomScramCredentialData(RANDOM);
    private static final ScramCredentialData ALPHA_SHA_512_FIRST = randomScramCredentialData(RANDOM);
    private static final ScramCredentialData GAMMA_SHA_512 = randomScramCredentialData(RANDOM);
    private static final ScramCredentialData ALPHA_SHA_256_SECOND = randomScramCredentialData(RANDOM);
    private static final ScramCredentialData ALPHA_SHA_512_SECOND = randomScramCredentialData(RANDOM);

    public static final ScramImage IMAGE1 = new ScramImage(Map.of(
            SCRAM_SHA_256, Map.of(
                    "alpha", ALPHA_SHA_256_FIRST,
                    "beta", BETA_SHA_256),
            SCRAM_SHA_512, Map.of(
                    "alpha", ALPHA_SHA_512_FIRST,
                    "gamma", GAMMA_SHA_512)
    ));

    public static final List<ApiMessageAndVersion> DELTA1_RECORDS = List.of(
            // remove all sha512 credentials
            new ApiMessageAndVersion(new RemoveUserScramCredentialRecord().
                    setName("alpha").
                    setMechanism(SCRAM_SHA_512.type()), (short) 0),
            new ApiMessageAndVersion(new RemoveUserScramCredentialRecord().
                    setName("gamma").
                    setMechanism(SCRAM_SHA_512.type()), (short) 0),
            // add sha256 credential
            new ApiMessageAndVersion(new UserScramCredentialRecord().
                    setName("alpha").
                    setMechanism(SCRAM_SHA_256.type()).
                    setSalt(ALPHA_SHA_256_SECOND.salt()).
                    setStoredKey(ALPHA_SHA_256_SECOND.storedKey()).
                    setServerKey(ALPHA_SHA_256_SECOND.serverKey()).
                    setIterations(ALPHA_SHA_256_SECOND.iterations()), (short) 0),
            // add sha512 credential re-using name
            new ApiMessageAndVersion(new UserScramCredentialRecord().
                    setName("alpha").
                    setMechanism(SCRAM_SHA_512.type()).
                    setSalt(ALPHA_SHA_512_SECOND.salt()).
                    setStoredKey(ALPHA_SHA_512_SECOND.storedKey()).
                    setServerKey(ALPHA_SHA_512_SECOND.serverKey()).
                    setIterations(ALPHA_SHA_512_SECOND.iterations()), (short) 0)
    );

    public static final ScramDelta DELTA1 = RecordTestUtils.replayAll(
            new ScramDelta(IMAGE1),
            DELTA1_RECORDS
    );

    public static final ScramImage IMAGE2 = new ScramImage(Map.of(
            SCRAM_SHA_256, Map.of(
                    "alpha", ALPHA_SHA_256_SECOND,
                    "beta", BETA_SHA_256),
            SCRAM_SHA_512, Map.of(
                    "alpha", ALPHA_SHA_512_SECOND)
    ));

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

    private ScramImageFixtures() {
    }
}
