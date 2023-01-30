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

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import joptsimple.AbstractOptionSpec;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionSpec;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateDelegationTokenOptions;
import org.apache.kafka.clients.admin.CreateDelegationTokenResult;
import org.apache.kafka.clients.admin.DescribeDelegationTokenOptions;
import org.apache.kafka.clients.admin.DescribeDelegationTokenResult;
import org.apache.kafka.clients.admin.ExpireDelegationTokenOptions;
import org.apache.kafka.clients.admin.ExpireDelegationTokenResult;
import org.apache.kafka.clients.admin.RenewDelegationTokenOptions;
import org.apache.kafka.clients.admin.RenewDelegationTokenResult;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.token.delegation.DelegationToken;
import org.apache.kafka.common.security.token.delegation.TokenInformation;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.SecurityUtils;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.util.CommandDefaultOptions;
import org.apache.kafka.server.util.CommandLineUtils;

public class DelegationTokenCommand {
    public static void main(String... args) {
        Exit.exit(mainNoExit(args));
    }

    static int mainNoExit(String... args) {
        try {
            execute(args);
            return 0;
        } catch (TerseException e) {
            System.err.println(e.getMessage());
            return 1;
        } catch (Throwable e) {
            System.err.println("Error while executing delegation token command : " + e.getMessage());
            System.err.println(Utils.stackTrace(e));
            return 1;
        }
    }

    static void execute(String... args) throws Exception {
        Admin adminClient = null;
        try {
            DelegationTokenCommandOptions opts = new DelegationTokenCommandOptions(args);
            CommandLineUtils.maybePrintHelpOrVersion(opts, "This tool helps to create, renew, expire, or describe delegation tokens.");

            // should have exactly one action
            int numberOfAction = 0;
            for (Boolean opt : new Boolean[]{opts.hasCreateOpt(), opts.hasRenewOpt(), opts.hasExpireOpt(), opts.hasDescribeOpt()}) {
                if (opt) {
                    numberOfAction++;
                }
            }
            if (numberOfAction != 1) {
                CommandLineUtils.printUsageAndExit(opts.parser, "Command must include exactly one action: --create, --renew, --expire or --describe");
            }

            opts.checkArgs();

            adminClient = createAdminClient(opts);

            if (opts.hasCreateOpt()) {
                createToken(adminClient, opts);
            } else if (opts.hasRenewOpt()) {
                renewToken(adminClient, opts);
            } else if (opts.hasExpireOpt()) {
                expireToken(adminClient, opts);
            } else if (opts.hasDescribeOpt()) {
                describeToken(adminClient, opts);
            }

        } finally {
            if (adminClient != null)
                adminClient.close();
        }
    }

    public static DelegationToken createToken(Admin adminClient, DelegationTokenCommandOptions opts) throws ExecutionException, InterruptedException {
        List<KafkaPrincipal> renewerPrincipals = getPrincipals(opts, opts.renewPrincipalsOpt);
        Long maxLifeTimeMs = opts.options.valueOf(opts.maxLifeTimeOpt);

        System.out.println("Calling create token operation with renewers :" + renewerPrincipals + " , max-life-time-period :" + maxLifeTimeMs);
        CreateDelegationTokenOptions createDelegationTokenOptions = new CreateDelegationTokenOptions().maxlifeTimeMs(maxLifeTimeMs).renewers(renewerPrincipals);

        List<KafkaPrincipal> ownerPrincipals = getPrincipals(opts, opts.ownerPrincipalsOpt);
        if (!ownerPrincipals.isEmpty()) {
            createDelegationTokenOptions.owner(ownerPrincipals.get(0));
        }

        CreateDelegationTokenResult createResult = adminClient.createDelegationToken(createDelegationTokenOptions);
        DelegationToken token = createResult.delegationToken().get();
        System.out.println("Created delegation token with tokenId : " + token.tokenInfo().tokenId());
        printToken(Collections.singletonList(token));

        return token;
    }

    private static void printToken(List<DelegationToken> tokens) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm");
        System.out.printf("%n%-15s %-30s %-15s %-15s %-25s %-15s %-15s %-15s%n", "TOKENID", "HMAC", "OWNER", "REQUESTER", "RENEWERS", "ISSUEDATE", "EXPIRYDATE", "MAXDATE");

        for (DelegationToken token : tokens) {
            TokenInformation tokenInfo = token.tokenInfo();
            System.out.printf("%n%-15s %-30s %-15s %-15s %-25s %-15s %-15s %-15s%n",
                    tokenInfo.tokenId(),
                    token.hmacAsBase64String(),
                    tokenInfo.owner(),
                    tokenInfo.tokenRequester(),
                    tokenInfo.renewersAsString(),
                    dateFormat.format(tokenInfo.issueTimestamp()),
                    dateFormat.format(tokenInfo.expiryTimestamp()),
                    dateFormat.format(tokenInfo.maxTimestamp()));
            System.out.println();
        }
    }

    private static List<KafkaPrincipal> getPrincipals(DelegationTokenCommandOptions opts, OptionSpec<String> principalOptionSpec) {
        List<KafkaPrincipal> principals = new ArrayList<>();

        if (opts.options.has(principalOptionSpec)) {
            for (Object e : opts.options.valuesOf(principalOptionSpec))
                principals.add(SecurityUtils.parseKafkaPrincipal(e.toString().trim()));
        }
        return principals;
    }

    public static Long renewToken(Admin adminClient, DelegationTokenCommandOptions opts) throws ExecutionException, InterruptedException {
        String hmac = opts.options.valueOf(opts.hmacOpt);
        Long renewTimePeriodMs = opts.options.valueOf(opts.renewTimePeriodOpt);

        System.out.println("Calling renew token operation with hmac :" + hmac + " , renew-time-period :" + renewTimePeriodMs);
        RenewDelegationTokenResult renewResult = adminClient.renewDelegationToken(Base64.getDecoder().decode(hmac), new RenewDelegationTokenOptions().renewTimePeriodMs(renewTimePeriodMs));
        Long expiryTimeStamp = renewResult.expiryTimestamp().get();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm");
        System.out.printf("Completed renew operation. New expiry date : %s", dateFormat.format(expiryTimeStamp));
        return expiryTimeStamp;
    }

    public static void expireToken(Admin adminClient, DelegationTokenCommandOptions opts) throws ExecutionException, InterruptedException {
        String hmac = opts.options.valueOf(opts.hmacOpt);
        Long expiryTimePeriodMs = opts.options.valueOf(opts.expiryTimePeriodOpt);

        System.out.println("Calling expire token operation with hmac :" + hmac + " , expire-time-period :" + expiryTimePeriodMs);
        ExpireDelegationTokenResult renewResult = adminClient.expireDelegationToken(Base64.getDecoder().decode(hmac), new ExpireDelegationTokenOptions().expiryTimePeriodMs(expiryTimePeriodMs));
        Long expiryTimeStamp = renewResult.expiryTimestamp().get();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm");
        System.out.printf("Completed expire operation. New expiry date : %s", dateFormat.format(expiryTimeStamp));
    }

    public static List<DelegationToken> describeToken(Admin adminClient, DelegationTokenCommandOptions opts) throws ExecutionException, InterruptedException {
        List<KafkaPrincipal> ownerPrincipals = getPrincipals(opts, opts.ownerPrincipalsOpt);

        if (ownerPrincipals.isEmpty()) {
            System.out.println("Calling describe token operation for current user.");
        } else {
            System.out.println("Calling describe token operation for owners :" + ownerPrincipals);
        }

        DescribeDelegationTokenResult describeResult = adminClient.describeDelegationToken(new DescribeDelegationTokenOptions().owners(ownerPrincipals));
        List<DelegationToken> tokens = describeResult.delegationTokens().get();
        System.out.printf("Total number of tokens : %d", tokens.size());
        printToken(tokens);
        return tokens;
    }

    private static Admin createAdminClient(DelegationTokenCommandOptions opts) throws IOException {
        Properties props = Utils.loadProps(opts.options.valueOf(opts.commandConfigOpt));
        props.put("bootstrap.servers", opts.options.valueOf(opts.bootstrapServerOpt));
        return Admin.create(props);
    }

    static class DelegationTokenCommandOptions extends CommandDefaultOptions {
        public final ArgumentAcceptingOptionSpec<String> bootstrapServerOpt;
        public final ArgumentAcceptingOptionSpec<String> commandConfigOpt;
        public final AbstractOptionSpec<Void> createOpt;
        public final AbstractOptionSpec<Void> renewOpt;
        public final AbstractOptionSpec<Void> expiryOpt;
        public final AbstractOptionSpec<Void> describeOpt;
        public final ArgumentAcceptingOptionSpec<String> ownerPrincipalsOpt;
        public final ArgumentAcceptingOptionSpec<String> renewPrincipalsOpt;
        public final ArgumentAcceptingOptionSpec<Long> maxLifeTimeOpt;
        public final ArgumentAcceptingOptionSpec<Long> renewTimePeriodOpt;
        public final ArgumentAcceptingOptionSpec<Long> expiryTimePeriodOpt;
        public final ArgumentAcceptingOptionSpec<String> hmacOpt;

        public DelegationTokenCommandOptions(String[] args) {
            super(args);

            String bootstrapServerDoc = "REQUIRED: server(s) to use for bootstrapping.";
            String commandConfigDoc = "REQUIRED: A property file containing configs to be passed to Admin Client. Token management" +
                    " operations are allowed in secure mode only. This config file is used to pass security related configs.";

            this.bootstrapServerOpt = parser.accepts("bootstrap-server", bootstrapServerDoc)
                    .withRequiredArg()
                    .ofType(String.class);

            this.commandConfigOpt = parser.accepts("command-config", commandConfigDoc)
                    .withRequiredArg()
                    .ofType(String.class);

            this.createOpt = parser.accepts("create", "Create a new delegation token. Use --renewer-principal option to pass renewers principals.");
            this.renewOpt = parser.accepts("renew", "Renew delegation token. Use --renew-time-period option to set renew time period.");
            this.expiryOpt = parser.accepts("expire", "Expire delegation token. Use --expiry-time-period option to expire the token.");
            this.describeOpt = parser.accepts("describe", "Describe delegation tokens for the given principals. Use --owner-principal to pass owner/renewer principals." +
                    " If --owner-principal option is not supplied, all the user owned tokens and tokens where user have Describe permission will be returned.");

            this.ownerPrincipalsOpt = parser.accepts("owner-principal", "owner is a kafka principal. It is should be in principalType:name format.")
                    .withOptionalArg()
                    .ofType(String.class);

            this.renewPrincipalsOpt = parser.accepts("renewer-principal", "renewer is a kafka principal. It is should be in principalType:name format.")
                    .withOptionalArg()
                    .ofType(String.class);

            this.maxLifeTimeOpt = parser.accepts("max-life-time-period", "Max life period for the token in milliseconds. If the value is -1," +
                            " then token max life time will default to a server side config the value (delegation.token.max.lifetime.ms).")
                    .withOptionalArg()
                    .ofType(Long.class);

            this.renewTimePeriodOpt = parser.accepts("renew-time-period", "Renew time period in milliseconds. If the value is -1, then the" +
                            " renew time period will default to a server side config the value (delegation.token.expiry.time.ms).")
                    .withOptionalArg()
                    .ofType(Long.class);

            this.expiryTimePeriodOpt = parser.accepts("expiry-time-period", "Expiry time period in milliseconds. If the value is -1, then the" +
                            " token will get invalidated immediately.")
                    .withOptionalArg()
                    .ofType(Long.class);

            this.hmacOpt = parser.accepts("hmac", "HMAC of the delegation token")
                    .withOptionalArg()
                    .ofType(String.class);

            options = parser.parse(args);
        }

        public boolean hasCreateOpt() {
            return options.has(createOpt);
        }

        public boolean hasRenewOpt() {
            return options.has(renewOpt);
        }

        public boolean hasExpireOpt() {
            return options.has(expiryOpt);
        }

        public boolean hasDescribeOpt() {
            return options.has(describeOpt);
        }

        public void checkArgs() {
            // check required args
            CommandLineUtils.checkRequiredArgs(parser, options, bootstrapServerOpt, commandConfigOpt);

            if (options.has(createOpt)) {
                CommandLineUtils.checkRequiredArgs(parser, options, maxLifeTimeOpt);
            }

            if (options.has(renewOpt)) {
                CommandLineUtils.checkRequiredArgs(parser, options, hmacOpt, renewTimePeriodOpt);
            }

            if (options.has(expiryOpt)) {
                CommandLineUtils.checkRequiredArgs(parser, options, hmacOpt, expiryTimePeriodOpt);
            }

            // check invalid args
            CommandLineUtils.checkInvalidArgs(parser, options, createOpt, new HashSet<>(Arrays.asList(hmacOpt, renewTimePeriodOpt, expiryTimePeriodOpt)));
            CommandLineUtils.checkInvalidArgs(parser, options, renewOpt, new HashSet<>(Arrays.asList(renewPrincipalsOpt, maxLifeTimeOpt, expiryTimePeriodOpt, ownerPrincipalsOpt)));
            CommandLineUtils.checkInvalidArgs(parser, options, expiryOpt, new HashSet<>(Arrays.asList(renewOpt, maxLifeTimeOpt, renewTimePeriodOpt, ownerPrincipalsOpt)));
            CommandLineUtils.checkInvalidArgs(parser, options, describeOpt, new HashSet<>(Arrays.asList(renewTimePeriodOpt, maxLifeTimeOpt, hmacOpt, renewTimePeriodOpt, expiryTimePeriodOpt)));
        }
    }
}


