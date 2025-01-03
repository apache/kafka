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
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.common.GroupType;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.util.CommandDefaultOptions;
import org.apache.kafka.server.util.CommandLineUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionSpec;
import joptsimple.OptionSpecBuilder;

public class GroupsCommand {
    private static final Logger LOG = LoggerFactory.getLogger(GroupsCommand.class);

    public static void main(String... args) {
        Exit.exit(mainNoExit(args));
    }

    static int mainNoExit(String... args) {
        try {
            execute(args);
            return 0;
        } catch (Throwable e) {
            System.err.println(e.getMessage());
            System.err.println(Utils.stackTrace(e));
            return 1;
        }
    }

    static void execute(String... args) throws Exception {
        GroupsCommandOptions opts = new GroupsCommandOptions(args);

        Properties config = opts.commandConfig();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, opts.bootstrapServer());

        int exitCode = 0;
        try (GroupsService service = new GroupsService(config)) {
            if (opts.hasListOption()) {
                service.listGroups(opts);
            }
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause != null) {
                printException(cause);
            } else {
                printException(e);
            }
            exitCode = 1;
        } catch (Throwable t) {
            printException(t);
            exitCode = 1;
        } finally {
            Exit.exit(exitCode);
        }
    }

    public static class GroupsService implements AutoCloseable {
        private final Admin adminClient;

        public GroupsService(Properties config) {
            this.adminClient = Admin.create(config);
        }

        // Visible for testing
        GroupsService(Admin adminClient) {
            this.adminClient = adminClient;
        }

        public void listGroups(GroupsCommandOptions opts) throws Exception {
            Collection<GroupListing> resources = adminClient.listGroups()
                    .all().get(30, TimeUnit.SECONDS);
            printGroupDetails(resources, opts.groupType(), opts.protocol(), opts.hasConsumerOption(), opts.hasShareOption());
        }

        private void printGroupDetails(Collection<GroupListing> groups,
                                       Optional<GroupType> groupTypeFilter,
                                       Optional<String> protocolFilter,
                                       boolean consumerGroupFilter,
                                       boolean shareGroupFilter) {
            List<List<String>> lineItems = new ArrayList<>();
            int maxLen = 20;
            for (GroupListing group : groups) {
                if (combinedFilter(group, groupTypeFilter, protocolFilter, consumerGroupFilter, shareGroupFilter)) {
                    List<String> lineItem = new ArrayList<>();
                    lineItem.add(group.groupId());
                    lineItem.add(group.type().map(GroupType::toString).orElse(""));
                    lineItem.add(group.protocol());
                    for (String item : lineItem) {
                        if (item != null) {
                            maxLen = Math.max(maxLen, item.length());
                        }
                    }
                    lineItems.add(lineItem);
                }
            }

            String fmt = "%" + (-maxLen) + "s";
            String header = fmt + " " + fmt + " " + fmt;
            System.out.printf(header, "GROUP", "TYPE", "PROTOCOL");
            System.out.println();
            for (List<String> item : lineItems) {
                for (String atom : item) {
                    System.out.printf(fmt + " ", atom);
                }
                System.out.println();
            }
        }

        private boolean combinedFilter(GroupListing group,
                                       Optional<GroupType> groupTypeFilter,
                                       Optional<String> protocolFilter,
                                       boolean consumerGroupFilter,
                                       boolean shareGroupFilter) {
            boolean pass = true;
            Optional<GroupType> groupType = group.type();
            String protocol = group.protocol();

            if (groupTypeFilter.isPresent()) {
                pass = groupType.filter(gt -> gt == groupTypeFilter.get()).isPresent()
                    && protocolFilter.map(protocol::equals).orElse(true);
            } else if (protocolFilter.isPresent()) {
                pass = protocol.equals(protocolFilter.get());
            } else if (consumerGroupFilter) {
                pass = protocol.equals("consumer") || protocol.isEmpty() || groupType.filter(gt -> gt == GroupType.CONSUMER).isPresent();
            } else if (shareGroupFilter) {
                pass = groupType.filter(gt -> gt == GroupType.SHARE).isPresent();
            }
            return pass;
        }

        @Override
        public void close() throws Exception {
            adminClient.close();
        }
    }

    private static void printException(Throwable e) {
        System.out.println("Error while executing groups command : " + e.getMessage());
        LOG.error(Utils.stackTrace(e));
    }

    public static final class GroupsCommandOptions extends CommandDefaultOptions {
        private final ArgumentAcceptingOptionSpec<String> bootstrapServerOpt;

        private final ArgumentAcceptingOptionSpec<String> commandConfigOpt;

        private final OptionSpecBuilder listOpt;

        private final ArgumentAcceptingOptionSpec<String> groupTypeOpt;

        private final ArgumentAcceptingOptionSpec<String> protocolOpt;

        private final OptionSpecBuilder consumerOpt;

        private final OptionSpecBuilder shareOpt;

        public GroupsCommandOptions(String[] args) {
            super(args);
            bootstrapServerOpt = parser.accepts("bootstrap-server", "REQUIRED: The Kafka server to connect to.")
                    .withRequiredArg()
                    .describedAs("server to connect to")
                    .required()
                    .ofType(String.class);
            commandConfigOpt = parser.accepts("command-config", "Property file containing configs to be passed to Admin Client.")
                    .withRequiredArg()
                    .describedAs("command config property file")
                    .ofType(String.class);

            listOpt = parser.accepts("list", "List the groups.");

            groupTypeOpt = parser.accepts("group-type", "Filter the groups based on group type. "
                            + "Valid types are: 'classic', 'consumer' and 'share'.")
                    .withRequiredArg()
                    .describedAs("type")
                    .ofType(String.class);

            protocolOpt = parser.accepts("protocol", "Filter the groups based on protocol type.")
                    .withRequiredArg()
                    .describedAs("protocol")
                    .ofType(String.class);

            consumerOpt = parser.accepts("consumer", "Filter the groups to show all kinds of consumer groups, including classic and simple consumer groups. "
                            + "This matches group type 'consumer', and group type 'classic' where the protocol type is 'consumer' or empty.");
            shareOpt = parser.accepts("share", "Filter the groups to show share groups.");

            options = parser.parse(args);

            checkArgs();
        }

        public Boolean has(OptionSpec<?> builder) {
            return options.has(builder);
        }

        public <A> Optional<A> valueAsOption(OptionSpec<A> option) {
            return valueAsOption(option, Optional.empty());
        }

        public <A> Optional<A> valueAsOption(OptionSpec<A> option, Optional<A> defaultValue) {
            if (has(option)) {
                return Optional.of(options.valueOf(option));
            } else {
                return defaultValue;
            }
        }

        public String bootstrapServer() {
            return options.valueOf(bootstrapServerOpt);
        }

        public Properties commandConfig() throws IOException {
            if (has(commandConfigOpt)) {
                return Utils.loadProps(options.valueOf(commandConfigOpt));
            } else {
                return new Properties();
            }
        }

        public Optional<GroupType> groupType() {
            return valueAsOption(groupTypeOpt).map(GroupType::parse).filter(gt -> gt != GroupType.UNKNOWN);
        }

        public Optional<String> protocol() {
            return valueAsOption(protocolOpt);
        }

        public boolean hasConsumerOption() {
            return has(consumerOpt);
        }

        public boolean hasListOption() {
            return has(listOpt);
        }

        public boolean hasShareOption() {
            return has(shareOpt);
        }

        public void checkArgs() {
            if (args.length == 0)
                CommandLineUtils.printUsageAndExit(parser, "This tool helps to list groups of all types.");

            CommandLineUtils.maybePrintHelpOrVersion(this, "This tool helps to list groups of all types.");

            // should have exactly one action
            long actions = Stream.of(listOpt).filter(options::has).count();
            if (actions != 1)
                CommandLineUtils.printUsageAndExit(parser, "Command must include exactly one action: --list.");

            if (has(groupTypeOpt)) {
                if (groupType().isEmpty()) {
                    throw new IllegalArgumentException("--group-type must be a valid group type.");
                }
            }

            // check invalid args
            CommandLineUtils.checkInvalidArgs(parser, options, consumerOpt, groupTypeOpt, protocolOpt, shareOpt);
            CommandLineUtils.checkInvalidArgs(parser, options, shareOpt, consumerOpt, groupTypeOpt, protocolOpt);
        }
    }
}
