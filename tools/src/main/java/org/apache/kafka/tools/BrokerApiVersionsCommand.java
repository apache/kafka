/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import net.sourceforge.argparse4j.inf.ArgumentParserException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import static org.apache.kafka.tools.BrokerApiVersionsCommand.BrokerVersionCommandOptions.COMMAND_CONFIG;

/**
 * A command for retrieving broker version information
 */
public class BrokerApiVersionsCommand {

    public static void main(String[] args) throws IOException {

        new BrokerApiVersionsCommand().run(args);
    }

    void run(String[] args) throws IOException {

        BrokerVersionCommandOptions opts = null;
        try {
            opts = new BrokerVersionCommandOptions(args);
        } catch (ArgumentParserException ape) {
            opts.parser.handleError(ape);
            Exit.exit(1);
        }

        opts.checkArgs();

        Properties props;
        if (opts.has(COMMAND_CONFIG))
            props = Utils.loadProps(opts.getCommandConfig());
        else
            props = new Properties();

        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, opts.getBootstrapServer());
        AdminClient adminClient = AdminClient.create(props);

        adminClient.listBrokersVersionInfo();


    }

    private static class BrokerVersionCommandOptions extends CommandOptions {

        public static final String COMMAND_CONFIG = "command-config";
        public static final String BOOTSTRAP_SERVER = "bootstrap-server";

        private BrokerVersionCommandOptions(String[] args) throws ArgumentParserException {
            super("broker-api-versions-command", "Retrieve broker version information");

            this.parser.addArgument("--command-config")
                    .required(true)
                    .type(String.class)
                    .help("A property file containing configs to be passed to Admin Client.");

            this.parser.addArgument("--bootstrap-server")
                    .required(true)
                    .type(String.class)
                    .help("REQUIRED: The server(s) to connect to.");

            this.ns = this.parser.parseArgs(args);
        }

        private String getCommandConfig() {
            return this.ns.getString(COMMAND_CONFIG);
        }

        private String getBootstrapServer() {
            return this.ns.getString(BOOTSTRAP_SERVER);
        }


        @Override
        public void checkArgs() throws IOException {
            CommandLineUtils.checkRequiredArgs(parser, this, Arrays.asList(BOOTSTRAP_SERVER));
        }
    }

}
