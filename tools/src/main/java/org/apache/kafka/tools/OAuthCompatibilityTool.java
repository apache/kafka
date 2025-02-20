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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.security.oauthbearer.internals.secured.AccessTokenRetriever;
import org.apache.kafka.common.security.oauthbearer.internals.secured.AccessTokenValidator;
import org.apache.kafka.common.security.oauthbearer.internals.secured.DefaultAccessTokenRetriever;
import org.apache.kafka.common.security.oauthbearer.internals.secured.DefaultAccessTokenValidator;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import java.util.List;
import java.util.Map;

import javax.security.auth.login.AppConfigurationEntry;

import static org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule.OAUTHBEARER_MECHANISM;

public class OAuthCompatibilityTool {

    public static void main(String[] args) {
        ArgumentParser parser = ArgumentParsers
            .newArgumentParser("oauth-compatibility-tool")
            .defaultHelp(true)
            .description(
                String.format(
                    "This tool is used to verify OAuth/OIDC provider compatibility.%n%n" +
                    "Run the following script to determine the configuration options:%n%n" +
                    "    ./bin/kafka-run-class.sh %s --help",
                    OAuthCompatibilityTool.class.getName()
                )
            );
        parser.addArgument("client-configuration-file")
            .type(String.class)
            .metavar("clientConfigurationFileName")
            .dest("clientConfigurationFileName")
            .help("Fully-qualified file name for the client configuration to use");
        parser.addArgument("broker-configuration-file")
            .type(String.class)
            .metavar("brokerConfigurationFileName")
            .dest("brokerConfigurationFileName")
            .help("Fully-qualified file name for the broker configuration to use");

        Namespace namespace;

        try {
            namespace = parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            Exit.exit(1);
            return;
        }

        try {
            String accessToken;

            // Client retrieval
            try (AccessTokenRetriever retriever = new DefaultAccessTokenRetriever();
                 AccessTokenValidator validator = new DefaultAccessTokenValidator()) {
                // Fill in the defaults for the values the user didn't specify.
                ConfigDef cd = new ConfigDef();
                SaslConfigs.addClientSaslSupport(cd);
                SslConfigs.addClientSslSupport(cd);
                String fileName = namespace.getString("clientConfigurationFileName");
                Map<String, ?> configs = new AbstractConfig(
                    cd,
                    Utils.propsToMap(Utils.loadProps(fileName))
                ).values();

                JaasContext context = JaasContext.loadClientContext(configs);
                List<AppConfigurationEntry> jaasConfigEntries = context.configurationEntries();

                retriever.configure(configs, OAUTHBEARER_MECHANISM, jaasConfigEntries);
                validator.configure(configs, OAUTHBEARER_MECHANISM, jaasConfigEntries);
                System.out.println("PASSED 1/5: client configuration");

                accessToken = retriever.retrieve();
                System.out.println("PASSED 2/5: client JWT retrieval");

                validator.validate(accessToken);
                System.out.println("PASSED 3/5: client JWT validation");
            }

            // Broker validation
            try (AccessTokenValidator validator = new DefaultAccessTokenValidator()) {
                String fileName = namespace.getString("brokerConfigurationFileName");
                Map<String, ?> configs = Utils.propsToMap(Utils.loadProps(fileName));
                JaasContext context = JaasContext.loadClientContext(configs);
                List<AppConfigurationEntry> jaasConfigEntries = context.configurationEntries();

                validator.configure(configs, OAUTHBEARER_MECHANISM, jaasConfigEntries);
                System.out.println("PASSED 4/5: broker configuration");

                validator.validate(accessToken);
                System.out.println("PASSED 5/5: broker JWT validation");
            }

            System.out.println("SUCCESS");
            Exit.exit(0);
        } catch (Throwable t) {
            System.out.println("FAILED:");
            t.printStackTrace(System.out);

            if (t instanceof ConfigException) {
                System.out.printf("%n");
                parser.printHelp();
            }

            Exit.exit(1);
        }
    }
}
