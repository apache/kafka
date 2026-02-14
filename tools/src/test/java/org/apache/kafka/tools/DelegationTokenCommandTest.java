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
        assertEquals(List.of(token1, token2), tokens);

        //get tokens for renewer2
        tokens = DelegationTokenCommand.describeToken(adminClient, getDescribeOpts(renewer2));
        assertEquals(1, tokens.size());
        assertEquals(List.of(token2), tokens);

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
    public void testCheckArgsMissingRequiredArgs() {
        Exit.setExitProcedure((exitCode, message) -> {
            throw new RuntimeException("Exit with code " + exitCode + ": " + message);
        });
        try {
            String[] argsCreateMissingBootstrap = {"--command-config", "testfile", "--create", "--max-life-time-period", "604800000"};
            DelegationTokenCommand.DelegationTokenCommandOptions optsCreateMissingBootstrap = new DelegationTokenCommand.DelegationTokenCommandOptions(argsCreateMissingBootstrap);
            assertThrows(RuntimeException.class, optsCreateMissingBootstrap::checkArgs);

            String[] argsCreateMissingConfig = {"--bootstrap-server", "localhost:9092", "--create", "--max-life-time-period", "604800000"};
            DelegationTokenCommand.DelegationTokenCommandOptions optsCreateMissingConfig = new DelegationTokenCommand.DelegationTokenCommandOptions(argsCreateMissingConfig);
            assertThrows(RuntimeException.class, optsCreateMissingConfig::checkArgs);

            String[] argsCreateMissingMaxLife = {"--bootstrap-server", "localhost:9092", "--command-config", "testfile", "--create"};
            DelegationTokenCommand.DelegationTokenCommandOptions optsCreateMissingMaxLife = new DelegationTokenCommand.DelegationTokenCommandOptions(argsCreateMissingMaxLife);
            assertThrows(RuntimeException.class, optsCreateMissingMaxLife::checkArgs);

            String[] argsRenewMissingBootstrap = {"--command-config", "testfile", "--renew", "--hmac", "test-hmac", "--renew-time-period", "604800000"};
            DelegationTokenCommand.DelegationTokenCommandOptions optsRenewMissingBootstrap = new DelegationTokenCommand.DelegationTokenCommandOptions(argsRenewMissingBootstrap);
            assertThrows(RuntimeException.class, optsRenewMissingBootstrap::checkArgs);

            String[] argsRenewMissingConfig = {"--bootstrap-server", "localhost:9092", "--renew", "--hmac", "test-hmac", "--renew-time-period", "604800000"};
            DelegationTokenCommand.DelegationTokenCommandOptions optsRenewMissingConfig = new DelegationTokenCommand.DelegationTokenCommandOptions(argsRenewMissingConfig);
            assertThrows(RuntimeException.class, optsRenewMissingConfig::checkArgs);

            String[] argsRenewMissingHmac = {"--bootstrap-server", "localhost:9092", "--command-config", "testfile", "--renew", "--renew-time-period", "604800000"};
            DelegationTokenCommand.DelegationTokenCommandOptions optsRenewMissingHmac = new DelegationTokenCommand.DelegationTokenCommandOptions(argsRenewMissingHmac);
            assertThrows(RuntimeException.class, optsRenewMissingHmac::checkArgs);

            String[] argsRenewMissingRenewTime = {"--bootstrap-server", "localhost:9092", "--command-config", "testfile", "--renew", "--hmac", "test-hmac"};
            DelegationTokenCommand.DelegationTokenCommandOptions optsRenewMissingRenewTime = new DelegationTokenCommand.DelegationTokenCommandOptions(argsRenewMissingRenewTime);
            assertThrows(RuntimeException.class, optsRenewMissingRenewTime::checkArgs);

            String[] argsExpireMissingBootstrap = {"--command-config", "testfile", "--expire", "--hmac", "test-hmac", "--expiry-time-period", "604800000"};
            DelegationTokenCommand.DelegationTokenCommandOptions optsExpireMissingBootstrap = new DelegationTokenCommand.DelegationTokenCommandOptions(argsExpireMissingBootstrap);
            assertThrows(RuntimeException.class, optsExpireMissingBootstrap::checkArgs);

            String[] argsExpireMissingConfig = {"--bootstrap-server", "localhost:9092", "--expire", "--hmac", "test-hmac", "--expiry-time-period", "604800000"};
            DelegationTokenCommand.DelegationTokenCommandOptions optsExpireMissingConfig = new DelegationTokenCommand.DelegationTokenCommandOptions(argsExpireMissingConfig);
            assertThrows(RuntimeException.class, optsExpireMissingConfig::checkArgs);

            String[] argsExpireMissingHmac = {"--bootstrap-server", "localhost:9092", "--command-config", "testfile", "--expire", "--expiry-time-period", "604800000"};
            DelegationTokenCommand.DelegationTokenCommandOptions optsExpireMissingHmac = new DelegationTokenCommand.DelegationTokenCommandOptions(argsExpireMissingHmac);
            assertThrows(RuntimeException.class, optsExpireMissingHmac::checkArgs);

            String[] argsExpireMissingExpiryTime = {"--bootstrap-server", "localhost:9092", "--command-config", "testfile", "--expire", "--hmac", "test-hmac"};
            DelegationTokenCommand.DelegationTokenCommandOptions optsExpireMissingExpiryTime = new DelegationTokenCommand.DelegationTokenCommandOptions(argsExpireMissingExpiryTime);
            assertThrows(RuntimeException.class, optsExpireMissingExpiryTime::checkArgs);

            String[] argsDescribeMissingBootstrap = {"--command-config", "testfile", "--describe"};
            DelegationTokenCommand.DelegationTokenCommandOptions optsDescribeMissingBootstrap = new DelegationTokenCommand.DelegationTokenCommandOptions(argsDescribeMissingBootstrap);
            assertThrows(RuntimeException.class, optsDescribeMissingBootstrap::checkArgs);

            String[] argsDescribeMissingConfig = {"--bootstrap-server", "localhost:9092", "--describe"};
            DelegationTokenCommand.DelegationTokenCommandOptions optsDescribeMissingConfig = new DelegationTokenCommand.DelegationTokenCommandOptions(argsDescribeMissingConfig);
            assertThrows(RuntimeException.class, optsDescribeMissingConfig::checkArgs);
        } finally {
            Exit.resetExitProcedure();
        }
    }

    @Test
    public void testCheckArgsInvalidArgs() {
        Exit.setExitProcedure((exitCode, message) -> {
            throw new RuntimeException("Exit with code " + exitCode + ": " + message);
        });
        try {
            String[] argsCreateWithHmac = {"--bootstrap-server", "localhost:9092", "--command-config", "testfile", "--create", "--max-life-time-period", "604800000", "--hmac", "test-hmac"};
            DelegationTokenCommand.DelegationTokenCommandOptions optsCreateWithHmac = new DelegationTokenCommand.DelegationTokenCommandOptions(argsCreateWithHmac);
            assertThrows(RuntimeException.class, optsCreateWithHmac::checkArgs);

            String[] argsCreateWithRenewTime = {"--bootstrap-server", "localhost:9092", "--command-config", "testfile", "--create", "--max-life-time-period", "604800000", "--renew-time-period", "604800000"};
            DelegationTokenCommand.DelegationTokenCommandOptions optsCreateWithRenewTime = new DelegationTokenCommand.DelegationTokenCommandOptions(argsCreateWithRenewTime);
            assertThrows(RuntimeException.class, optsCreateWithRenewTime::checkArgs);

            String[] argsCreateWithExpiryTime = {"--bootstrap-server", "localhost:9092", "--command-config", "testfile", "--create", "--max-life-time-period", "604800000", "--expiry-time-period", "604800000"};
            DelegationTokenCommand.DelegationTokenCommandOptions optsCreateWithExpiryTime = new DelegationTokenCommand.DelegationTokenCommandOptions(argsCreateWithExpiryTime);
            assertThrows(RuntimeException.class, optsCreateWithExpiryTime::checkArgs);

            String[] argsRenewWithRenewPrincipals = {"--bootstrap-server", "localhost:9092", "--command-config", "testfile", "--renew", "--hmac", "test-hmac", "--renew-time-period", "604800000", "--renewer-principal", "User:renewer"};
            DelegationTokenCommand.DelegationTokenCommandOptions optsRenewWithRenewPrincipals = new DelegationTokenCommand.DelegationTokenCommandOptions(argsRenewWithRenewPrincipals);
            assertThrows(RuntimeException.class, optsRenewWithRenewPrincipals::checkArgs);

            String[] argsRenewWithMaxLife = {"--bootstrap-server", "localhost:9092", "--command-config", "testfile", "--renew", "--hmac", "test-hmac", "--renew-time-period", "604800000", "--max-life-time-period", "604800000"};
            DelegationTokenCommand.DelegationTokenCommandOptions optsRenewWithMaxLife = new DelegationTokenCommand.DelegationTokenCommandOptions(argsRenewWithMaxLife);
            assertThrows(RuntimeException.class, optsRenewWithMaxLife::checkArgs);

            String[] argsRenewWithExpiryTime = {"--bootstrap-server", "localhost:9092", "--command-config", "testfile", "--renew", "--hmac", "test-hmac", "--renew-time-period", "604800000", "--expiry-time-period", "604800000"};
            DelegationTokenCommand.DelegationTokenCommandOptions optsRenewWithExpiryTime = new DelegationTokenCommand.DelegationTokenCommandOptions(argsRenewWithExpiryTime);
            assertThrows(RuntimeException.class, optsRenewWithExpiryTime::checkArgs);

            String[] argsRenewWithOwner = {"--bootstrap-server", "localhost:9092", "--command-config", "testfile", "--renew", "--hmac", "test-hmac", "--renew-time-period", "604800000", "--owner-principal", "User:owner"};
            DelegationTokenCommand.DelegationTokenCommandOptions optsRenewWithOwner = new DelegationTokenCommand.DelegationTokenCommandOptions(argsRenewWithOwner);
            assertThrows(RuntimeException.class, optsRenewWithOwner::checkArgs);

            String[] argsExpireWithRenew = {"--bootstrap-server", "localhost:9092", "--command-config", "testfile", "--expire", "--renew", "--hmac", "test-hmac", "--expiry-time-period", "604800000"};
            DelegationTokenCommand.DelegationTokenCommandOptions optsExpireWithRenew = new DelegationTokenCommand.DelegationTokenCommandOptions(argsExpireWithRenew);
            assertThrows(RuntimeException.class, optsExpireWithRenew::checkArgs);

            String[] argsExpireWithMaxLife = {"--bootstrap-server", "localhost:9092", "--command-config", "testfile", "--expire", "--hmac", "test-hmac", "--expiry-time-period", "604800000", "--max-life-time-period", "604800000"};
            DelegationTokenCommand.DelegationTokenCommandOptions optsExpireWithMaxLife = new DelegationTokenCommand.DelegationTokenCommandOptions(argsExpireWithMaxLife);
            assertThrows(RuntimeException.class, optsExpireWithMaxLife::checkArgs);

            String[] argsExpireWithRenewTime = {"--bootstrap-server", "localhost:9092", "--command-config", "testfile", "--expire", "--hmac", "test-hmac", "--expiry-time-period", "604800000", "--renew-time-period", "604800000"};
            DelegationTokenCommand.DelegationTokenCommandOptions optsExpireWithRenewTime = new DelegationTokenCommand.DelegationTokenCommandOptions(argsExpireWithRenewTime);
            assertThrows(RuntimeException.class, optsExpireWithRenewTime::checkArgs);

            String[] argsExpireWithOwner = {"--bootstrap-server", "localhost:9092", "--command-config", "testfile", "--expire", "--hmac", "test-hmac", "--expiry-time-period", "604800000", "--owner-principal", "User:owner"};
            DelegationTokenCommand.DelegationTokenCommandOptions optsExpireWithOwner = new DelegationTokenCommand.DelegationTokenCommandOptions(argsExpireWithOwner);
            assertThrows(RuntimeException.class, optsExpireWithOwner::checkArgs);

            String[] argsDescribeWithRenewTime = {"--bootstrap-server", "localhost:9092", "--command-config", "testfile", "--describe", "--renew-time-period", "604800000"};
            DelegationTokenCommand.DelegationTokenCommandOptions optsDescribeWithRenewTime = new DelegationTokenCommand.DelegationTokenCommandOptions(argsDescribeWithRenewTime);
            assertThrows(RuntimeException.class, optsDescribeWithRenewTime::checkArgs);

            String[] argsDescribeWithExpiryTime = {"--bootstrap-server", "localhost:9092", "--command-config", "testfile", "--describe", "--expiry-time-period", "604800000"};
            DelegationTokenCommand.DelegationTokenCommandOptions optsDescribeWithExpiryTime = new DelegationTokenCommand.DelegationTokenCommandOptions(argsDescribeWithExpiryTime);
            assertThrows(RuntimeException.class, optsDescribeWithExpiryTime::checkArgs);

            String[] argsDescribeWithMaxLife = {"--bootstrap-server", "localhost:9092", "--command-config", "testfile", "--describe", "--max-life-time-period", "604800000"};
            DelegationTokenCommand.DelegationTokenCommandOptions optsDescribeWithMaxLife = new DelegationTokenCommand.DelegationTokenCommandOptions(argsDescribeWithMaxLife);
            assertThrows(RuntimeException.class, optsDescribeWithMaxLife::checkArgs);

            String[] argsDescribeWithHmac = {"--bootstrap-server", "localhost:9092", "--command-config", "testfile", "--describe", "--hmac", "test-hmac"};
            DelegationTokenCommand.DelegationTokenCommandOptions optsDescribeWithHmac = new DelegationTokenCommand.DelegationTokenCommandOptions(argsDescribeWithHmac);
            assertThrows(RuntimeException.class, optsDescribeWithHmac::checkArgs);
        } finally {
            Exit.resetExitProcedure();
        }
    }

    @Test
    public void testCheckArgsValidOperations() {
        String[] argsCreate = {"--bootstrap-server", "localhost:9092", "--command-config", "testfile", "--create", "--max-life-time-period", "604800000"};
        DelegationTokenCommand.DelegationTokenCommandOptions optsCreate = new DelegationTokenCommand.DelegationTokenCommandOptions(argsCreate);
        optsCreate.checkArgs();

        String[] argsRenew = {"--bootstrap-server", "localhost:9092", "--command-config", "testfile", "--renew", "--hmac", "test-hmac", "--renew-time-period", "604800000"};
        DelegationTokenCommand.DelegationTokenCommandOptions optsRenew = new DelegationTokenCommand.DelegationTokenCommandOptions(argsRenew);
        optsRenew.checkArgs();

        String[] argsExpire = {"--bootstrap-server", "localhost:9092", "--command-config", "testfile", "--expire", "--hmac", "test-hmac", "--expiry-time-period", "604800000"};
        DelegationTokenCommand.DelegationTokenCommandOptions optsExpire = new DelegationTokenCommand.DelegationTokenCommandOptions(argsExpire);
        optsExpire.checkArgs();

        String[] argsDescribe = {"--bootstrap-server", "localhost:9092", "--command-config", "testfile", "--describe"};
        DelegationTokenCommand.DelegationTokenCommandOptions optsDescribe = new DelegationTokenCommand.DelegationTokenCommandOptions(argsDescribe);
        optsDescribe.checkArgs();
    }
}
