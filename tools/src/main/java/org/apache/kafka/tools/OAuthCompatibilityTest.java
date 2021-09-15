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

import java.util.HashMap;
import java.util.Map;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.security.oauthbearer.secured.AccessTokenRetriever;
import org.apache.kafka.common.security.oauthbearer.secured.AccessTokenValidator;
import org.apache.kafka.common.security.oauthbearer.secured.CloseableVerificationKeyResolver;
import org.apache.kafka.common.security.oauthbearer.secured.LoginCallbackHandlerConfiguration;
import org.apache.kafka.common.security.oauthbearer.secured.OAuthBearerLoginCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.secured.OAuthBearerValidatorCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.secured.ValidatorCallbackHandlerConfiguration;
import org.apache.kafka.common.utils.Exit;

public class OAuthCompatibilityTest {

    public static void main(String[] args) {
        String description = String.format(
            "This tool is used to verify OAuth/OIDC provider compatibility.%n%n" +
            "To use, first export KAFKA_OPTS with Java system properties that match your%n" +
            "OAuth/OIDC configuration. Next, run the following script to execute the test:%n%n" +
            "    ./bin/kafka-run-class.sh %s" +
            "%n%n" +
            "Please refer to the following source files for OAuth/OIDC client and broker%n" +
            "configuration options:" +
            "%n%n" +
            "    %s%n" +
            "    %s",
            OAuthCompatibilityTest.class.getName(),
            LoginCallbackHandlerConfiguration.class.getName(),
            ValidatorCallbackHandlerConfiguration.class.getName());

        ArgumentParser parser = ArgumentParsers
            .newArgumentParser("oauth-compatibility-test")
            .defaultHelp(true)
            .description(description);

        try {
            parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            Exit.exit(1);
            return;
        }

        Map<String, Object> options = new HashMap<>();

        // Copy over the options from the system properties for our configuration. Let's
        // assume/hope/pray there are no conflicts.
        for (Map.Entry<Object, Object> entry : System.getProperties().entrySet())
            options.put((String) entry.getKey(), entry.getValue());

        try {
            String accessToken;

            {
                LoginCallbackHandlerConfiguration conf = new LoginCallbackHandlerConfiguration(options);
                AccessTokenRetriever atr = OAuthBearerLoginCallbackHandler.configureAccessTokenRetriever(conf);
                AccessTokenValidator atv = OAuthBearerLoginCallbackHandler.configureAccessTokenValidator(conf);
                System.out.println("PASSED 1/5: client configuration");

                accessToken = atr.retrieve();
                System.out.println("PASSED 2/5: client JWT retrieval");

                atv.validate(accessToken);
                System.out.println("PASSED 3/5: client JWT validation");
            }

            {
                ValidatorCallbackHandlerConfiguration conf = new ValidatorCallbackHandlerConfiguration(options);

                try (CloseableVerificationKeyResolver vkr = OAuthBearerValidatorCallbackHandler.configureVerificationKeyResolver(conf)) {
                    AccessTokenValidator atv = OAuthBearerValidatorCallbackHandler.configureAccessTokenValidator(conf, vkr);
                    System.out.println("PASSED 4/5: broker configuration");

                    atv.validate(accessToken);
                    System.out.println("PASSED 5/5: broker JWT validation");
                }
            }

            System.out.println("SUCCESS");
            Exit.exit(0);
        } catch (Throwable t) {
            System.out.println("FAILED:");
            t.printStackTrace();

            if (t instanceof ConfigException) {
                System.out.printf("%n");
                parser.printHelp();
            }

            Exit.exit(1);
        }
    }

}
