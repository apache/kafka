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

import org.apache.kafka.common.metadata.DelegationTokenRecord;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.token.delegation.TokenInformation;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

@Timeout(value = 40)
public class DelegationTokenDataTest {

    private static final List<String> UUID = Arrays.asList(
        Uuid.randomUuid().toString(),
        Uuid.randomUuid().toString(),
        Uuid.randomUuid().toString());

    private static final List<KafkaPrincipal> EMPTYRENEWERS = Arrays.asList();

    private static final List<TokenInformation> TOKENINFORMATION = Arrays.asList(
        new TokenInformation(
            UUID.get(0),
            new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "alice"),
            new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "alice"),
            EMPTYRENEWERS,
            0,
            100,
            100),
        new TokenInformation(
            UUID.get(1),
            new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "alice"),
            new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "alice"),
            EMPTYRENEWERS,
            0,
            100,
            100),
        new TokenInformation(
            UUID.get(2),
            new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "fred"),
            new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "alice"),
            EMPTYRENEWERS,
            0,
            100,
            100));

    private static final List<DelegationTokenData> DELEGATIONTOKENDATA = Arrays.asList(
        new DelegationTokenData(TOKENINFORMATION.get(0)),
        new DelegationTokenData(TOKENINFORMATION.get(1)),
        new DelegationTokenData(TOKENINFORMATION.get(2)));

    @Test
    public void testValues() {
        assertEquals(TOKENINFORMATION.get(0), DELEGATIONTOKENDATA.get(0).tokenInformation());
        assertEquals(TOKENINFORMATION.get(1), DELEGATIONTOKENDATA.get(1).tokenInformation());
        assertEquals(TOKENINFORMATION.get(2), DELEGATIONTOKENDATA.get(2).tokenInformation());
    }

    @Test
    public void testEquals() {
        assertNotEquals(DELEGATIONTOKENDATA.get(0), DELEGATIONTOKENDATA.get(1));
        assertNotEquals(DELEGATIONTOKENDATA.get(1), DELEGATIONTOKENDATA.get(0));
        assertNotEquals(DELEGATIONTOKENDATA.get(0), DELEGATIONTOKENDATA.get(2));
        assertNotEquals(DELEGATIONTOKENDATA.get(2), DELEGATIONTOKENDATA.get(0));
        assertEquals(DELEGATIONTOKENDATA.get(0), DELEGATIONTOKENDATA.get(0));
        assertEquals(DELEGATIONTOKENDATA.get(1), DELEGATIONTOKENDATA.get(1));
        assertEquals(DELEGATIONTOKENDATA.get(2), DELEGATIONTOKENDATA.get(2));
    }

    @Test
    public void testToString() {
        assertEquals("DelegationTokenData" +
            "(tokenInformation=" + "[hidden]" +
            ")", DELEGATIONTOKENDATA.get(0).toString());
    }

    @Test
    public void testFromRecordAndToRecord() {
        testRoundTrip(DELEGATIONTOKENDATA.get(0));
        testRoundTrip(DELEGATIONTOKENDATA.get(1));
        testRoundTrip(DELEGATIONTOKENDATA.get(2));
    }

    private void testRoundTrip(DelegationTokenData origDelegationTokenData) {
        ApiMessageAndVersion messageAndVersion = new ApiMessageAndVersion(
            origDelegationTokenData.toRecord(), (short) 0);
        DelegationTokenData newDelegationTokenData = DelegationTokenData.fromRecord(
            (DelegationTokenRecord) messageAndVersion.message());
        assertEquals(origDelegationTokenData, newDelegationTokenData);
        ApiMessageAndVersion messageAndVersion2 = new ApiMessageAndVersion(
            newDelegationTokenData.toRecord(), (short) 0);
        assertEquals(messageAndVersion, messageAndVersion2);
    }
}
