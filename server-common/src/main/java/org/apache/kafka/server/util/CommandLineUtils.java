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
package org.apache.kafka.server.util;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.Exit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

/**
 * Helper functions for dealing with command line utilities.
 */
public class CommandLineUtils {
    /**
     * Check if there are no options or `--help` option from command line.
     *
     * @param commandOpts Acceptable options for a command
     * @return true on matching the help check condition
     */
    public static boolean isPrintHelpNeeded(CommandDefaultOptions commandOpts) {
        return commandOpts.args.length == 0 || commandOpts.options.has(commandOpts.helpOpt);
    }

    /**
     * Check if there is `--version` option from command line.
     *
     * @param commandOpts Acceptable options for a command
     * @return true on matching the help check condition
     */
    public static boolean isPrintVersionNeeded(CommandDefaultOptions commandOpts) {
        return commandOpts.options.has(commandOpts.versionOpt);
    }

    /**
     * Check and print help message if there is no options or `--help` option
     * from command line, if `--version` is specified on the command line
     * print version information and exit.
     *
     * @param commandOpts Acceptable options for a command
     * @param message     Message to display on successful check
     */
    public static void maybePrintHelpOrVersion(CommandDefaultOptions commandOpts, String message) {
        if (isPrintHelpNeeded(commandOpts)) {
            printUsageAndExit(commandOpts.parser, message);
        }
        if (isPrintVersionNeeded(commandOpts)) {
            printVersionAndExit();
        }
    }

    /**
     * Check that all the listed options are present.
     */
    public static void checkRequiredArgs(OptionParser parser, OptionSet options, OptionSpec<?>... requiredList) {
        for (OptionSpec<?> arg : requiredList) {
            if (!options.has(arg)) {
                printUsageAndExit(parser, String.format("Missing required argument \"%s\"", arg));
            }
        }
    }

    /**
     * Check that none of the listed options are present.
     */
    public static void checkInvalidArgs(OptionParser parser,
                                        OptionSet options,
                                        OptionSpec<?> usedOption,
                                        OptionSpec<?>... invalidOptions) {
        if (options.has(usedOption)) {
            for (OptionSpec<?> arg : invalidOptions) {
                if (options.has(arg)) {
                    printUsageAndExit(parser, String.format("Option \"%s\" can't be used with option \"%s\"", usedOption, arg));
                }
            }
        }
    }

    /**
     * Check that none of the listed options are present.
     */
    public static void checkInvalidArgs(OptionParser parser,
                                        OptionSet options,
                                        OptionSpec<?> usedOption,
                                        Set<OptionSpec<?>> invalidOptions) {
        OptionSpec<?>[] array = new OptionSpec<?>[invalidOptions.size()];
        invalidOptions.toArray(array);
        checkInvalidArgs(parser, options, usedOption, array);
    }

    /**
     * Check that none of the listed options are present with the combination of used options.
     */
    public static void checkInvalidArgsSet(OptionParser parser,
                                           OptionSet options,
                                           Set<OptionSpec<?>> usedOptions,
                                           Set<OptionSpec<?>> invalidOptions,
                                           Optional<String> trailingAdditionalMessage) {
        if (usedOptions.stream().filter(options::has).count() == usedOptions.size()) {
            for (OptionSpec<?> arg : invalidOptions) {
                if (options.has(arg)) {
                    printUsageAndExit(parser, String.format("Option combination \"%s\" can't be used with option \"%s\"%s",
                            usedOptions, arg, trailingAdditionalMessage.orElse("")));
                }
            }
        }
    }

    public static void printUsageAndExit(OptionParser parser, String message) {
        System.err.println(message);
        try {
            parser.printHelpOn(System.err);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Exit.exit(1, message);
    }

    public static void printVersionAndExit() {
        System.out.println(AppInfoParser.getVersion());
        Exit.exit(0);
    }

    /**
     * Parse key-value pairs in the form key=value.
     * Value may contain equals sign.
     */
    public static Properties parseKeyValueArgs(List<String> args) {
        return parseKeyValueArgs(args, true);
    }

    /**
     * Parse key-value pairs in the form key=value.
     * Value may contain equals sign.
     */
    public static Properties parseKeyValueArgs(List<String> args, boolean acceptMissingValue) {
        Properties props = new Properties();
        List<String[]> splits = new ArrayList<>();
        args.forEach(arg -> {
            String[] split = arg.split("=", 2);
            if (split.length > 0) {
                splits.add(split);
            }
        });
        splits.forEach(split -> {
            if (split.length == 1 || (split.length == 2 && (split[1] == null || split[1].isEmpty()))) {
                if (acceptMissingValue) {
                    props.put(split[0], "");
                } else {
                    throw new IllegalArgumentException(String.format("Missing value for key %s}", split[0]));
                }
            } else {
                props.put(split[0], split[1]);
            }
        });
        return props;
    }

    /**
     * Merge the options into {@code props} for key {@code key}, with the following precedence, from high to low:
     * 1) if {@code spec} is specified on {@code options} explicitly, use the value;
     * 2) if {@code props} already has {@code key} set, keep it;
     * 3) otherwise, use the default value of {@code spec}.
     * A {@code null} value means to remove {@code key} from the {@code props}.
     */
    public static <T> void maybeMergeOptions(Properties props, String key, OptionSet options, OptionSpec<T> spec) {
        if (options.has(spec) || !props.containsKey(key)) {
            T value = options.valueOf(spec);
            if (value == null) {
                props.remove(key);
            } else {
                props.put(key, value.toString());
            }
        }
    }

    static class InitializeBootstrapException extends RuntimeException {
        private final static long serialVersionUID = 1L;

        InitializeBootstrapException(String message) {
            super(message);
        }
    }

    public static void initializeBootstrapProperties(
        Properties properties,
        Optional<String> bootstrapServer,
        Optional<String> bootstrapControllers
    ) {
        if (bootstrapServer.isPresent()) {
            if (bootstrapControllers.isPresent()) {
                throw new InitializeBootstrapException("You cannot specify both " +
                        "--bootstrap-controller and --bootstrap-server.");
            }
            properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                    bootstrapServer.get());
            properties.remove(AdminClientConfig.BOOTSTRAP_CONTROLLERS_CONFIG);
        } else if (bootstrapControllers.isPresent()) {
            properties.remove(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
            properties.setProperty(AdminClientConfig.BOOTSTRAP_CONTROLLERS_CONFIG,
                    bootstrapControllers.get());
        } else {
            throw new InitializeBootstrapException("You must specify either --bootstrap-controller " +
                    "or --bootstrap-server.");
        }
    }

    public static void initializeBootstrapProperties(
        OptionParser parser,
        OptionSet options,
        Properties properties,
        OptionSpec<String> bootstrapServer,
        OptionSpec<String> bootstrapControllers
    ) {
        try {
            initializeBootstrapProperties(properties,
                options.has(bootstrapServer) ?
                    Optional.of(options.valueOf(bootstrapServer)) : Optional.empty(),
                options.has(bootstrapControllers) ?
                        Optional.of(options.valueOf(bootstrapControllers)) : Optional.empty());
        } catch (InitializeBootstrapException e) {
            printUsageAndExit(parser, e.getMessage());
        }
    }
}
