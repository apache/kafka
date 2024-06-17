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
package org.apache.kafka.tools;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.common.security.token.delegation.DelegationToken;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DelegationTokenCommandTest {

    @Test
    public void testDelegationTokenRequests() throws ExecutionException, InterruptedException {
        Admin adminClient = new MockAdminClient.Builder().build();

        String renewer1 = "User:renewer1";
        String renewer2 = "User:renewer2";

        // create token1 with renewer1
        DelegationToken tokenCreated = DelegationTokenCommand.createToken(adminClient, getCreateOpts(renewer1));

        List<DelegationToken> tokens = DelegationTokenCommand.describeToken(adminClient, getDescribeOpts(""));
        assertEquals(1, tokens.size());
        DelegationToken token1 = tokens.get(0);
        assertEquals(token1, tokenCreated);

        // create token2 with renewer2
        DelegationToken token2 = DelegationTokenCommand.createToken(adminClient, getCreateOpts(renewer2));

        tokens = DelegationTokenCommand.describeToken(adminClient, getDescribeOpts(""));
        assertEquals(2, tokens.size());
        assertEquals(Arrays.asList(token1, token2), tokens);

        //get tokens for renewer2
        tokens = DelegationTokenCommand.describeToken(adminClient, getDescribeOpts(renewer2));
        assertEquals(1, tokens.size());
        assertEquals(Collections.singletonList(token2), tokens);

        //test renewing tokens
        Long expiryTimestamp = DelegationTokenCommand.renewToken(adminClient, getRenewOpts(token1.hmacAsBase64String()));
        DelegationToken renewedToken = DelegationTokenCommand.describeToken(adminClient, getDescribeOpts(renewer1)).get(0);
        assertEquals(expiryTimestamp, renewedToken.tokenInfo().expiryTimestamp());

        //test expire tokens
        DelegationTokenCommand.expireToken(adminClient, getExpireOpts(token1.hmacAsBase64String()));
        DelegationTokenCommand.expireToken(adminClient, getExpireOpts(token2.hmacAsBase64String()));

        tokens = DelegationTokenCommand.describeToken(adminClient, getDescribeOpts(""));
        assertEquals(0, tokens.size());

        //create token with invalid renewer principal type
        assertThrows(ExecutionException.class, () -> DelegationTokenCommand.createToken(adminClient, getCreateOpts("Group:Renewer3")));

        // try describing tokens for unknown owner
        assertTrue(DelegationTokenCommand.describeToken(adminClient, getDescribeOpts("User:Unknown")).isEmpty());

    }

    private DelegationTokenCommand.DelegationTokenCommandOptions getCreateOpts(String renewer) {
        String[] args = {"--bootstrap-server", "localhost:9092", "--max-life-time-period", "-1", "--command-config", "testfile", "--create", "--renewer-principal", renewer};
        return new DelegationTokenCommand.DelegationTokenCommandOptions(args);
    }

    private DelegationTokenCommand.DelegationTokenCommandOptions getDescribeOpts(String owner) {
        List<String> args = new ArrayList<>();
        args.add("--bootstrap-server");
        args.add("localhost:9092");
        args.add("--command-config");
        args.add("testfile");
        args.add("--describe");
        if (!owner.isEmpty()) {
            args.add("--owner-principal");
            args.add(owner);
        }
        return new DelegationTokenCommand.DelegationTokenCommandOptions(args.toArray(new String[0]));
    }

    private DelegationTokenCommand.DelegationTokenCommandOptions getRenewOpts(String hmac) {
        String[] args = {"--bootstrap-server", "localhost:9092", "--command-config", "testfile", "--renew", "--renew-time-period", "604800000", "--hmac", hmac};
        return new DelegationTokenCommand.DelegationTokenCommandOptions(args);
    }

    private DelegationTokenCommand.DelegationTokenCommandOptions getExpireOpts(String hmac) {
        String[] args = {"--bootstrap-server", "localhost:9092", "--command-config", "testfile", "--expire", "--expiry-time-period", "-1", "--hmac", hmac};
        return new DelegationTokenCommand.DelegationTokenCommandOptions(args);
    }
}
