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
import org.apache.kafka.common.utils.internals.SecurityUtils;
import org.apache.kafka.metadata.DelegationTokenData;
import org.apache.kafka.metadata.RecordTestUtils;
import org.apache.kafka.server.common.ApiMessageAndVersion;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class DelegationTokenImageTestFixtures {
    public static final DelegationTokenImage IMAGE1;
    public static final List<ApiMessageAndVersion> DELTA1_RECORDS;
    public static final DelegationTokenDelta DELTA1;
    public static final DelegationTokenImage IMAGE2;

    private static DelegationTokenData randomDelegationTokenData(String tokenId, long expireTimestamp) {
        TokenInformation ti = new TokenInformation(
            tokenId,
            SecurityUtils.parseKafkaPrincipal(KafkaPrincipal.USER_TYPE + ":" + "fred"),
            SecurityUtils.parseKafkaPrincipal(KafkaPrincipal.USER_TYPE + ":" + "fred"),
            new ArrayList<>(),
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

    private DelegationTokenImageTestFixtures() {
    }
}
