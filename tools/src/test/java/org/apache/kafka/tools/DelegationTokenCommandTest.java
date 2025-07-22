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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.common.security.token.delegation.DelegationToken;
import org.apache.kafka.common.utils.Exit;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

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

    @Test
    public void testCheckArgsCreateOperation() {
        String[] args = {"--bootstrap-server", "localhost:9092", "--command-config", "testfile", "--create", "--max-life-time-period", "604800000"};
        DelegationTokenCommand.DelegationTokenCommandOptions opts = new DelegationTokenCommand.DelegationTokenCommandOptions(args);
        
        opts.checkArgs();
    }

    @Test
    public void testCheckArgsRenewOperation() {
        String[] args = {"--bootstrap-server", "localhost:9092", "--command-config", "testfile", "--renew", "--hmac", "test-hmac", "--renew-time-period", "604800000"};
        DelegationTokenCommand.DelegationTokenCommandOptions opts = new DelegationTokenCommand.DelegationTokenCommandOptions(args);
        
        opts.checkArgs();
    }

    @Test
    public void testCheckArgsExpireOperation() {
        String[] args = {"--bootstrap-server", "localhost:9092", "--command-config", "testfile", "--expire", "--hmac", "test-hmac", "--expiry-time-period", "604800000"};
        DelegationTokenCommand.DelegationTokenCommandOptions opts = new DelegationTokenCommand.DelegationTokenCommandOptions(args);
        
        opts.checkArgs();
    }

    @Test
    public void testCheckArgsDescribeOperation() {
        String[] args = {"--bootstrap-server", "localhost:9092", "--command-config", "testfile", "--describe"};
        DelegationTokenCommand.DelegationTokenCommandOptions opts = new DelegationTokenCommand.DelegationTokenCommandOptions(args);
        
        opts.checkArgs();
    }

    @Test
    public void testCheckArgsInvalidArgsForCreate() {
        String[] args = {"--bootstrap-server", "localhost:9092", "--command-config", "testfile", "--create", "--max-life-time-period", "604800000", "--hmac", "test-hmac"};
        DelegationTokenCommand.DelegationTokenCommandOptions opts = new DelegationTokenCommand.DelegationTokenCommandOptions(args);
        
        Exit.setExitProcedure((exitCode, message) -> {
            throw new RuntimeException("Exit with code " + exitCode + ": " + message);
        });
        try {
            assertThrows(RuntimeException.class, () -> opts.checkArgs());
        } finally {
            Exit.resetExitProcedure();
        }
    }

    @Test
    public void testCheckArgsInvalidArgsForRenew() {
        String[] args = {"--bootstrap-server", "localhost:9092", "--command-config", "testfile", "--renew", "--hmac", "test-hmac", "--renew-time-period", "604800000", "--max-life-time-period", "604800000"};
        DelegationTokenCommand.DelegationTokenCommandOptions opts = new DelegationTokenCommand.DelegationTokenCommandOptions(args);
        
        Exit.setExitProcedure((exitCode, message) -> {
            throw new RuntimeException("Exit with code " + exitCode + ": " + message);
        });
        try {
            assertThrows(RuntimeException.class, () -> opts.checkArgs());
        } finally {
            Exit.resetExitProcedure();
        }
    }

    @Test
    public void testCheckArgsInvalidArgsForExpire() {
        String[] args = {"--bootstrap-server", "localhost:9092", "--command-config", "testfile", "--expire", "--hmac", "test-hmac", "--expiry-time-period", "604800000", "--renew-time-period", "604800000"};
        DelegationTokenCommand.DelegationTokenCommandOptions opts = new DelegationTokenCommand.DelegationTokenCommandOptions(args);

        Exit.setExitProcedure((exitCode, message) -> {
            throw new RuntimeException("Exit with code " + exitCode + ": " + message);
        });
        try {
            assertThrows(RuntimeException.class, () -> opts.checkArgs());
        } finally {
            Exit.resetExitProcedure();
        }
    }

    @Test
    public void testCheckArgsInvalidArgsForDescribe() {
        String[] args = {"--bootstrap-server", "localhost:9092", "--command-config", "testfile", "--describe", "--renew-time-period", "604800000"};
        DelegationTokenCommand.DelegationTokenCommandOptions opts = new DelegationTokenCommand.DelegationTokenCommandOptions(args);
        
        Exit.setExitProcedure((exitCode, message) -> {
            throw new RuntimeException("Exit with code " + exitCode + ": " + message);
        });
        try {
            assertThrows(RuntimeException.class, () -> opts.checkArgs());
        } finally {
            Exit.resetExitProcedure();
        }
    }

    @Test
    public void testCheckArgsMissingRequiredArgs() {
        Exit.setExitProcedure((exitCode, message) -> {
            throw new RuntimeException("Exit with code " + exitCode + ": " + message);
        });
        try {
            String[] args1 = {"--command-config", "testfile", "--create", "--max-life-time-period", "604800000"};
            DelegationTokenCommand.DelegationTokenCommandOptions opts1 = new DelegationTokenCommand.DelegationTokenCommandOptions(args1);
            assertThrows(RuntimeException.class, () -> opts1.checkArgs());

            String[] args2 = {"--bootstrap-server", "localhost:9092", "--create", "--max-life-time-period", "604800000"};
            DelegationTokenCommand.DelegationTokenCommandOptions opts2 = new DelegationTokenCommand.DelegationTokenCommandOptions(args2);
            assertThrows(RuntimeException.class, () -> opts2.checkArgs());

            String[] args3 = {"--bootstrap-server", "localhost:9092", "--command-config", "testfile", "--create"};
            DelegationTokenCommand.DelegationTokenCommandOptions opts3 = new DelegationTokenCommand.DelegationTokenCommandOptions(args3);
            assertThrows(RuntimeException.class, () -> opts3.checkArgs());

            String[] args4 = {"--bootstrap-server", "localhost:9092", "--command-config", "testfile", "--renew", "--renew-time-period", "604800000"};
            DelegationTokenCommand.DelegationTokenCommandOptions opts4 = new DelegationTokenCommand.DelegationTokenCommandOptions(args4);
            assertThrows(RuntimeException.class, () -> opts4.checkArgs());

            String[] args5 = {"--bootstrap-server", "localhost:9092", "--command-config", "testfile", "--renew", "--hmac", "test-hmac"};
            DelegationTokenCommand.DelegationTokenCommandOptions opts5 = new DelegationTokenCommand.DelegationTokenCommandOptions(args5);
            assertThrows(RuntimeException.class, () -> opts5.checkArgs());

            String[] args6 = {"--bootstrap-server", "localhost:9092", "--command-config", "testfile", "--expire", "--expiry-time-period", "604800000"};
            DelegationTokenCommand.DelegationTokenCommandOptions opts6 = new DelegationTokenCommand.DelegationTokenCommandOptions(args6);
            assertThrows(RuntimeException.class, () -> opts6.checkArgs());

            String[] args7 = {"--bootstrap-server", "localhost:9092", "--command-config", "testfile", "--expire", "--hmac", "test-hmac"};
            DelegationTokenCommand.DelegationTokenCommandOptions opts7 = new DelegationTokenCommand.DelegationTokenCommandOptions(args7);
            assertThrows(RuntimeException.class, () -> opts7.checkArgs());

            String[] args8 = {"--command-config", "testfile", "--describe"};
            DelegationTokenCommand.DelegationTokenCommandOptions opts8 = new DelegationTokenCommand.DelegationTokenCommandOptions(args8);
            assertThrows(RuntimeException.class, () -> opts8.checkArgs());

            String[] args9 = {"--bootstrap-server", "localhost:9092", "--describe"};
            DelegationTokenCommand.DelegationTokenCommandOptions opts9 = new DelegationTokenCommand.DelegationTokenCommandOptions(args9);
            assertThrows(RuntimeException.class, () -> opts9.checkArgs());
        } finally {
            Exit.resetExitProcedure();
        }
    }
}
