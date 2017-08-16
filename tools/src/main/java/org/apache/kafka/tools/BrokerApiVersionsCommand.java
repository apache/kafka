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

import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListBrokersVersionInfoResult;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.utils.Utils;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * A command for retrieving broker version information
 */
public class BrokerApiVersionsCommand {

    public static void main(String[] args) throws IOException {
        new BrokerApiVersionsCommand().run(args);
    }

    void run(String[] args) throws IOException {
        BrokerVersionCommandOptions opts = new BrokerVersionCommandOptions(args);
        Properties props = opts.getCommandConfig() == null ? new Properties() : Utils.loadProps(opts.getCommandConfig());
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, opts.getBootstrapServer());
        AdminClient adminClient = AdminClient.create(props);
        try {
            requestAndPrintBrokersVersionInfo(adminClient);
        } finally {
            adminClient.close();
        }
    }

    private void requestAndPrintBrokersVersionInfo(AdminClient adminClient) {
        ListBrokersVersionInfoResult listBrokersVersionInfoResult = adminClient.listBrokersVersionInfo();
        for (Map.Entry<Node, KafkaFutureImpl<NodeApiVersions>> entry : listBrokersVersionInfoResult.getBrokerVersionsInfoFutures().entrySet()) {
            try {
                NodeApiVersions nodeApiVersions = entry.getValue().get();
                print(entry.getKey(), nodeApiVersions.toString(true));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (ExecutionException e) {
                print(entry.getKey(), e.getMessage());
            }
        }
    }

    private void print(Node node, String message) {
        System.out.println(String.format("%s -> %s", node, message));
    }

    private static class BrokerVersionCommandOptions extends CommandOptions {

        private BrokerVersionCommandOptions(String[] args) {
            super("broker-api-versions-command", "Retrieve broker version information", args);
        }

        private String getCommandConfig() {
            return this.ns.getString("command-config");
        }

        private String getBootstrapServer() {
            return this.ns.getString("bootstrap-server");
        }

        @Override
        public void prepareArgs() {
            this.parser.addArgument("--command-config")
                    .required(false)
                    .type(String.class)
                    .help("A property file containing configs to be passed to Admin Client.");

            this.parser.addArgument("--bootstrap-server")
                    .required(true)
                    .type(String.class)
                    .help("REQUIRED: The server(s) to connect to.");
        }

    }

}
