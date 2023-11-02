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

package org.apache.kafka.metadata;

import org.apache.kafka.clients.admin.ScramMechanism;
import org.apache.kafka.common.metadata.UserScramCredentialRecord;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.util.MockRandom;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

@Timeout(value = 40)
public class ScramCredentialDataTest {

    static MockRandom random = new MockRandom();

    static byte[] randomBuffer(Random random, int length) {
        byte[] buf = new byte[length];
        random.nextBytes(buf);
        return buf;
    }

    private static final List<ScramCredentialData> SCRAMCREDENTIALDATA = Arrays.asList(
        new ScramCredentialData(
            randomBuffer(random, 1024),
            randomBuffer(random, 1024),
            randomBuffer(random, 1024),
            4096),
        new ScramCredentialData(
            randomBuffer(random, 1024),
            randomBuffer(random, 1024),
            randomBuffer(random, 1024),
            8192),
        new ScramCredentialData(
            randomBuffer(random, 1024),
            randomBuffer(random, 1024),
            randomBuffer(random, 1024),
            10000));

    @Test
    public void testValues() {
        assertEquals(4096, SCRAMCREDENTIALDATA.get(0).iterations());
        assertEquals(8192, SCRAMCREDENTIALDATA.get(1).iterations());
        assertEquals(10000, SCRAMCREDENTIALDATA.get(2).iterations());
    }

    @Test
    public void testEquals() {
        assertNotEquals(SCRAMCREDENTIALDATA.get(0), SCRAMCREDENTIALDATA.get(1));
        assertNotEquals(SCRAMCREDENTIALDATA.get(1), SCRAMCREDENTIALDATA.get(0));
        assertNotEquals(SCRAMCREDENTIALDATA.get(0), SCRAMCREDENTIALDATA.get(2));
        assertNotEquals(SCRAMCREDENTIALDATA.get(2), SCRAMCREDENTIALDATA.get(0));
        assertEquals(SCRAMCREDENTIALDATA.get(0), SCRAMCREDENTIALDATA.get(0));
        assertEquals(SCRAMCREDENTIALDATA.get(1), SCRAMCREDENTIALDATA.get(1));
        assertEquals(SCRAMCREDENTIALDATA.get(2), SCRAMCREDENTIALDATA.get(2));
    }

    @Test
    public void testToString() {
        assertEquals("ScramCredentialData" +
            "(salt=" + "[hidden]" +
            ", storedKey=" + "[hidden]" +
            ", serverKey=" + "[hidden]" +
            ", iterations=" + "[hidden]" +
            ")", SCRAMCREDENTIALDATA.get(0).toString());
    }

    @Test
    public void testFromRecordAndToRecord() {
        testRoundTrip(SCRAMCREDENTIALDATA.get(0));
        testRoundTrip(SCRAMCREDENTIALDATA.get(1));
        testRoundTrip(SCRAMCREDENTIALDATA.get(2));
    }

    private void testRoundTrip(ScramCredentialData scramCredentialData) {
        ApiMessageAndVersion messageAndVersion = new ApiMessageAndVersion(
            scramCredentialData.toRecord("alice", ScramMechanism.SCRAM_SHA_256), (short) 0);
        ScramCredentialData scramCredentialData2 = ScramCredentialData.fromRecord(
            (UserScramCredentialRecord) messageAndVersion.message());
        assertEquals(scramCredentialData, scramCredentialData2);
        ApiMessageAndVersion messageAndVersion2 = new ApiMessageAndVersion(
            scramCredentialData2.toRecord("alice", ScramMechanism.SCRAM_SHA_256), (short) 0);
        assertEquals(messageAndVersion, messageAndVersion2);
    }

}
