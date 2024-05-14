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

import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionSpec;
import joptsimple.OptionSpecBuilder;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsOptions;
import org.apache.kafka.clients.admin.ClientMetricsResourceListing;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.util.CommandDefaultOptions;
import org.apache.kafka.server.util.CommandLineUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ClientMetricsCommand {
    private static final Logger LOG = LoggerFactory.getLogger(ClientMetricsCommand.class);

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
        ClientMetricsCommandOptions opts = new ClientMetricsCommandOptions(args);

        Properties config = opts.commandConfig();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, opts.bootstrapServer());

        int exitCode = 0;
        try (ClientMetricsService service = new ClientMetricsService(config)) {
            if (opts.hasAlterOption()) {
                service.alterClientMetrics(opts);
            } else if (opts.hasDescribeOption()) {
                service.describeClientMetrics(opts);
            } else if (opts.hasDeleteOption()) {
                service.deleteClientMetrics(opts);
            } else if (opts.hasListOption()) {
                service.listClientMetrics();
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

    public static class ClientMetricsService implements AutoCloseable {
        private final Admin adminClient;

        public ClientMetricsService(Properties config) {
            this.adminClient = Admin.create(config);
        }

        // Visible for testing
        ClientMetricsService(Admin adminClient) {
            this.adminClient = adminClient;
        }

        public void alterClientMetrics(ClientMetricsCommandOptions opts) throws Exception {
            String entityName = opts.hasGenerateNameOption() ? Uuid.randomUuid().toString() : opts.name().get();

            Map<String, String> configsToBeSet = new HashMap<>();
            opts.interval().map(intervalVal -> configsToBeSet.put("interval.ms", intervalVal.toString()));
            opts.metrics().map(metricslist -> configsToBeSet.put("metrics", String.join(",", metricslist)));
            opts.match().map(matchlist -> configsToBeSet.put("match", String.join(",", matchlist)));

            ConfigResource configResource = new ConfigResource(ConfigResource.Type.CLIENT_METRICS, entityName);
            AlterConfigsOptions alterOptions = new AlterConfigsOptions().timeoutMs(30000).validateOnly(false);
            Collection<AlterConfigOp> alterEntries = configsToBeSet.entrySet().stream()
                    .map(entry -> new AlterConfigOp(new ConfigEntry(entry.getKey(), entry.getValue()),
                            entry.getValue().isEmpty() ? AlterConfigOp.OpType.DELETE : AlterConfigOp.OpType.SET))
                    .collect(Collectors.toList());
            adminClient.incrementalAlterConfigs(Collections.singletonMap(configResource, alterEntries), alterOptions).all()
                    .get(30, TimeUnit.SECONDS);

            System.out.println("Altered client metrics config for " + entityName + ".");
        }

        public void deleteClientMetrics(ClientMetricsCommandOptions opts) throws Exception {
            String entityName = opts.name().get();
            Collection<ConfigEntry> oldConfigs = getClientMetricsConfig(entityName);

            ConfigResource configResource = new ConfigResource(ConfigResource.Type.CLIENT_METRICS, entityName);
            AlterConfigsOptions alterOptions = new AlterConfigsOptions().timeoutMs(30000).validateOnly(false);
            Collection<AlterConfigOp> alterEntries = oldConfigs.stream()
                    .map(entry -> new AlterConfigOp(entry, AlterConfigOp.OpType.DELETE)).collect(Collectors.toList());
            adminClient.incrementalAlterConfigs(Collections.singletonMap(configResource, alterEntries), alterOptions)
                    .all().get(30, TimeUnit.SECONDS);

            System.out.println("Deleted client metrics config for " + entityName + ".");
        }

        public void describeClientMetrics(ClientMetricsCommandOptions opts) throws Exception {
            Optional<String> entityNameOpt = opts.name();

            List<String> entities;
            if (entityNameOpt.isPresent()) {
                entities = Collections.singletonList(entityNameOpt.get());
            } else {
                Collection<ClientMetricsResourceListing> resources = adminClient.listClientMetricsResources()
                        .all().get(30, TimeUnit.SECONDS);
                entities = resources.stream().map(ClientMetricsResourceListing::name).collect(Collectors.toList());
            }

            for (String entity : entities) {
                System.out.println("Client metrics configs for " + entity + " are:");
                getClientMetricsConfig(entity)
                        .forEach(entry -> System.out.println("  " + entry.name() + "=" + entry.value()));
            }
        }

        public void listClientMetrics() throws Exception {
            Collection<ClientMetricsResourceListing> resources = adminClient.listClientMetricsResources()
                    .all().get(30, TimeUnit.SECONDS);
            String results = resources.stream().map(ClientMetricsResourceListing::name).collect(Collectors.joining("\n"));
            System.out.println(results);
        }

        private Collection<ConfigEntry> getClientMetricsConfig(String entityName) throws Exception {
            ConfigResource configResource = new ConfigResource(ConfigResource.Type.CLIENT_METRICS, entityName);
            Map<ConfigResource, Config> result = adminClient.describeConfigs(Collections.singleton(configResource))
                    .all().get(30, TimeUnit.SECONDS);
            return result.get(configResource).entries();
        }

        @Override
        public void close() throws Exception {
            adminClient.close();
        }
    }

    private static void printException(Throwable e) {
        System.out.println("Error while executing client metrics command : " + e.getMessage());
        LOG.error(Utils.stackTrace(e));
    }

    public static final class ClientMetricsCommandOptions extends CommandDefaultOptions {
        private final ArgumentAcceptingOptionSpec<String> bootstrapServerOpt;

        private final ArgumentAcceptingOptionSpec<String> commandConfigOpt;

        private final OptionSpecBuilder alterOpt;

        private final OptionSpecBuilder deleteOpt;

        private final OptionSpecBuilder describeOpt;

        private final OptionSpecBuilder listOpt;

        private final ArgumentAcceptingOptionSpec<String> nameOpt;

        private final OptionSpecBuilder generateNameOpt;

        private final ArgumentAcceptingOptionSpec<Integer> intervalOpt;

        private final ArgumentAcceptingOptionSpec<String> matchOpt;

        private final ArgumentAcceptingOptionSpec<String> metricsOpt;
  
        public ClientMetricsCommandOptions(String[] args) {
            super(args);
            bootstrapServerOpt = parser.accepts("bootstrap-server", "REQUIRED: The Kafka server to connect to.")
                .withRequiredArg()
                .describedAs("server to connect to")
                .ofType(String.class);
            commandConfigOpt = parser.accepts("command-config", "Property file containing configs to be passed to Admin Client.")
                .withRequiredArg()
                .describedAs("command config property file")
                .ofType(String.class);

            alterOpt = parser.accepts("alter", "Alter the configuration for the client metrics resource.");
            deleteOpt = parser.accepts("delete", "Delete the configuration for the client metrics resource.");
            describeOpt = parser.accepts("describe", "List configurations for the client metrics resource.");
            listOpt = parser.accepts("list", "List the client metrics resources.");

            nameOpt = parser.accepts("name", "Name of client metrics configuration resource.")
                .withRequiredArg()
                .describedAs("name")
                .ofType(String.class);
            generateNameOpt = parser.accepts("generate-name", "Generate a UUID to use as the name.");
            intervalOpt = parser.accepts("interval", "The metrics push interval in milliseconds.")
                .withRequiredArg()
                .describedAs("push interval")
                .ofType(java.lang.Integer.class);

            String nl = System.lineSeparator();

            String[] matchSelectors = new String[] {
                "client_id", "client_instance_id", "client_software_name",
                "client_software_version", "client_source_address", "client_source_port"
            };
            String matchSelectorNames = Arrays.stream(matchSelectors).map(config -> "\t" + config).collect(Collectors.joining(nl));
            matchOpt = parser.accepts("match",  "Matching selector 'k1=v1,k2=v2'. The following is a list of valid selector names: " + nl + matchSelectorNames)
                .withRequiredArg()
                .describedAs("k1=v1,k2=v2")
                .ofType(String.class)
                .withValuesSeparatedBy(',');
            metricsOpt = parser.accepts("metrics",  "Telemetry metric name prefixes 'm1,m2'.")
                .withRequiredArg()
                .describedAs("m1,m2")
                .ofType(String.class)
                .withValuesSeparatedBy(',');

            options = parser.parse(args);

            checkArgs();
        }

        public Boolean has(OptionSpec<?> builder) {
            return options.has(builder);
        }

        public <A> Optional<A> valueAsOption(OptionSpec<A> option) {
            return valueAsOption(option, Optional.empty());
        }

        public <A> Optional<List<A>> valuesAsOption(OptionSpec<A> option) {
            return valuesAsOption(option, Optional.empty());
        }

        public <A> Optional<A> valueAsOption(OptionSpec<A> option, Optional<A> defaultValue) {
            if (has(option)) {
                return Optional.of(options.valueOf(option));
            } else {
                return defaultValue;
            }
        }

        public <A> Optional<List<A>> valuesAsOption(OptionSpec<A> option, Optional<List<A>> defaultValue) {
            return options.has(option) ? Optional.of(options.valuesOf(option)) : defaultValue;
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

        public boolean hasAlterOption() {
            return has(alterOpt);
        }

        public boolean hasDeleteOption() {
            return has(deleteOpt);
        }

        public boolean hasDescribeOption() {
            return has(describeOpt);
        }

        public boolean hasListOption() {
            return has(listOpt);
        }

        public Optional<String> name() {
            return valueAsOption(nameOpt);
        }

        public boolean hasGenerateNameOption() {
            return has(generateNameOpt);
        }

        public Optional<List<String>> metrics() {
            return valuesAsOption(metricsOpt);
        }

        public Optional<Integer> interval() {
            return valueAsOption(intervalOpt);
        }

        public Optional<List<String>> match() {
            return valuesAsOption(matchOpt);
        }

        public void checkArgs() {
            if (args.length == 0)
                CommandLineUtils.printUsageAndExit(parser, "This tool helps to manipulate and describe client metrics configurations.");

            CommandLineUtils.maybePrintHelpOrVersion(this, "This tool helps to manipulate and describe client metrics configurations.");

            // should have exactly one action
            long actions = Stream.of(alterOpt, deleteOpt, describeOpt, listOpt).filter(options::has).count();
            if (actions != 1)
                CommandLineUtils.printUsageAndExit(parser, "Command must include exactly one action: --alter, --delete, --describe or --list.");

            // check required args
            if (!has(bootstrapServerOpt))
                throw new IllegalArgumentException("--bootstrap-server must be specified.");

            // check invalid args
            CommandLineUtils.checkInvalidArgs(parser, options, deleteOpt, generateNameOpt, intervalOpt, matchOpt, metricsOpt);
            CommandLineUtils.checkInvalidArgs(parser, options, describeOpt, generateNameOpt, intervalOpt, matchOpt, metricsOpt);
            CommandLineUtils.checkInvalidArgs(parser, options, listOpt, nameOpt, generateNameOpt, intervalOpt, matchOpt, metricsOpt);
          
            boolean isNamePresent = has(nameOpt);

            if (has(alterOpt)) {
                if ((isNamePresent && has(generateNameOpt)) || (!isNamePresent && !has(generateNameOpt)))
                    throw new IllegalArgumentException("One of --name or --generate-name must be specified with --alter.");
            }

            if (has(deleteOpt) && !isNamePresent)
                throw new IllegalArgumentException("A client metrics resource name must be specified with --delete.");
        }
    }
}