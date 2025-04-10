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
package org.apache.kafka.tools.streams;

import org.apache.kafka.server.util.CommandDefaultOptions;
import org.apache.kafka.server.util.CommandLineUtils;

import joptsimple.OptionSpec;

public class StreamsGroupCommandOptions extends CommandDefaultOptions {
    public static final String BOOTSTRAP_SERVER_DOC = "REQUIRED: The server(s) to connect to.";
    public static final String LIST_DOC = "List all streams groups.";
    public static final String TIMEOUT_MS_DOC = "The timeout that can be set for some use cases. For example, it can be used when describing the group " +
        "to specify the maximum amount of time in milliseconds to wait before the group stabilizes.";
    public static final String COMMAND_CONFIG_DOC = "Property file containing configs to be passed to Admin Client.";
    public static final String STATE_DOC = "When specified with '--list', it displays the state of all groups. It can also be used to list groups with specific states. " +
        "Valid values are Empty, NotReady, Stable, Assigning, Reconciling, and Dead.";

    public final OptionSpec<String> bootstrapServerOpt;
    public final OptionSpec<Void> listOpt;
    public final OptionSpec<Long> timeoutMsOpt;
    public final OptionSpec<String> commandConfigOpt;
    public final OptionSpec<String> stateOpt;


    public StreamsGroupCommandOptions(String[] args) {
        super(args);

        bootstrapServerOpt = parser.accepts("bootstrap-server", BOOTSTRAP_SERVER_DOC)
            .withRequiredArg()
            .describedAs("server to connect to")
            .ofType(String.class);
        listOpt = parser.accepts("list", LIST_DOC);
        timeoutMsOpt = parser.accepts("timeout", TIMEOUT_MS_DOC)
            .withRequiredArg()
            .describedAs("timeout (ms)")
            .ofType(Long.class)
            .defaultsTo(5000L);
        commandConfigOpt = parser.accepts("command-config", COMMAND_CONFIG_DOC)
            .withRequiredArg()
            .describedAs("command config property file")
            .ofType(String.class);
        stateOpt = parser.accepts("state", STATE_DOC)
            .availableIf(listOpt)
            .withOptionalArg()
            .ofType(String.class);

        options = parser.parse(args);
    }

    public void checkArgs() {
        CommandLineUtils.maybePrintHelpOrVersion(this, "This tool helps to list streams groups.");

        CommandLineUtils.checkRequiredArgs(parser, options, bootstrapServerOpt);
    }
}
