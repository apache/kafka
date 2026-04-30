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

    private static final List<ScramCredentialData> SCRAMCREDENTIALDATA = List.of(
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

    @Test
    public void testEqualsAndHashCode() {
        byte[] salt1 = {1, 2, 3};
        byte[] storedKey1 = {4, 5, 6};
        byte[] serverKey1 = {7, 8, 9};
        int iterations1 = 1000;

        byte[] salt2 = {1, 2, 3};
        byte[] storedKey2 = {4, 5, 6};
        byte[] serverKey2 = {7, 8, 9};
        int iterations2 = 1000;

        ScramCredentialData data1 = new ScramCredentialData(salt1, storedKey1, serverKey1, iterations1);
        ScramCredentialData data2 = new ScramCredentialData(salt2, storedKey2, serverKey2, iterations2);

        assertEquals(data1, data2);
        assertEquals(data1.hashCode(), data2.hashCode());
    }

    @Test
    public void testNotEqualsDifferentContent() {
        byte[] salt1 = {1, 2, 3};
        byte[] storedKey1 = {4, 5, 6};
        byte[] serverKey1 = {7, 8, 9};
        int iterations1 = 1000;

        byte[] salt2 = {9, 8, 7};
        byte[] storedKey2 = {6, 5, 4};
        byte[] serverKey2 = {3, 2, 1};
        int iterations2 = 2000;

        ScramCredentialData data1 = new ScramCredentialData(salt1, storedKey1, serverKey1, iterations1);
        ScramCredentialData data2 = new ScramCredentialData(salt2, storedKey2, serverKey2, iterations2);

        assertNotEquals(data1, data2);
        assertNotEquals(data1.hashCode(), data2.hashCode());
    }

    @Test
    public void testEqualsSameInstance() {
        byte[] salt = {1, 2, 3};
        byte[] storedKey = {4, 5, 6};
        byte[] serverKey = {7, 8, 9};
        int iterations = 1000;

        ScramCredentialData data = new ScramCredentialData(salt, storedKey, serverKey, iterations);

        // Test equals method for same instance
        assertEquals(data, data);
        assertEquals(data.hashCode(), data.hashCode());
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
