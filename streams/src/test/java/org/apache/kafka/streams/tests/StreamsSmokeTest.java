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
package org.apache.kafka.streams.tests;

import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamsConfig;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import static org.apache.kafka.streams.tests.SmokeTestDriver.generate;
import static org.apache.kafka.streams.tests.SmokeTestDriver.generatePerpetually;

public class StreamsSmokeTest {

    /**
     *  args ::= kafka propFileName command disableAutoTerminate
     *  command := "run" | "process"
     *
     * @param args
     */
    @SuppressWarnings("deprecation")
    public static void main(final String[] args) throws IOException {
        if (args.length < 2) {
            System.err.println("StreamsSmokeTest are expecting two parameters: propFile, command; but only see " + args.length + " parameter");
            Exit.exit(1);
        }

        final String propFileName = args[0];
        final String command = args[1];
        final boolean disableAutoTerminate = args.length > 2;

        final Properties streamsProperties = Utils.loadProps(propFileName);
        final String kafka = streamsProperties.getProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG);
        final String processingGuarantee = streamsProperties.getProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG);

        if (kafka == null) {
            System.err.println("No bootstrap kafka servers specified in " + StreamsConfig.BOOTSTRAP_SERVERS_CONFIG);
            Exit.exit(1);
        }

        if ("process".equals(command)) {
            if (!StreamsConfig.AT_LEAST_ONCE.equals(processingGuarantee) &&
                !StreamsConfig.EXACTLY_ONCE.equals(processingGuarantee) &&
                !StreamsConfig.EXACTLY_ONCE_BETA.equals(processingGuarantee) &&
                !StreamsConfig.EXACTLY_ONCE_V2.equals(processingGuarantee)) {

                System.err.println("processingGuarantee must be either " +
                                       StreamsConfig.AT_LEAST_ONCE + ", " +
                                       StreamsConfig.EXACTLY_ONCE + ", or " +
                                       StreamsConfig.EXACTLY_ONCE_BETA + ", or " +
                                       StreamsConfig.EXACTLY_ONCE_V2);

                Exit.exit(1);
            }
        }

        System.out.println("StreamsTest instance started (StreamsSmokeTest)");
        System.out.println("command=" + command);
        System.out.println("props=" + streamsProperties);
        System.out.println("disableAutoTerminate=" + disableAutoTerminate);

        switch (command) {
            case "run":
                // this starts the driver (data generation and result verification)
                final int numKeys = 20;
                final int maxRecordsPerKey = 1000;
                if (disableAutoTerminate) {
                    generatePerpetually(kafka, numKeys, maxRecordsPerKey);
                } else {
                    // slow down data production so that system tests have time to
                    // do their bounces, etc.
                    final Map<String, Set<Integer>> allData =
                        generate(kafka, numKeys, maxRecordsPerKey, Duration.ofSeconds(90));
                    SmokeTestDriver.verify(kafka, allData, maxRecordsPerKey);
                }
                break;
            case "process":
                // this starts the stream processing app
                new SmokeTestClient(UUID.randomUUID().toString()).start(streamsProperties);
                break;
            case "close-deadlock-test":
                final ShutdownDeadlockTest test = new ShutdownDeadlockTest(kafka);
                test.start();
                break;
            default:
                System.out.println("unknown command: " + command);
        }
    }

}
