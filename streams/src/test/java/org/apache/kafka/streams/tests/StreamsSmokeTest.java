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

import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamsConfig;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class StreamsSmokeTest {

    /**
     *  args ::= kafka propFileName command disableAutoTerminate
     *  command := "run" | "process"
     *
     * @param args
     */
    public static void main(final String[] args) throws InterruptedException, IOException {
        if (args.length < 2) {
            System.err.println("StreamsSmokeTest are expecting two parameters: propFile, command; but only see " + args.length + " parameter");
            System.exit(1);
        }

        final String propFileName = args[0];
        final String command = args[1];
        final boolean disableAutoTerminate = args.length > 3;

        final Properties streamsProperties = Utils.loadProps(propFileName);
        final String kafka = streamsProperties.getProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG);

        if (kafka == null) {
            System.err.println("No bootstrap kafka servers specified in " + StreamsConfig.BOOTSTRAP_SERVERS_CONFIG);
            System.exit(1);
        }

        System.out.println("StreamsTest instance started (StreamsSmokeTest)");
        System.out.println("command=" + command);
        System.out.println("props=" + streamsProperties);
        System.out.println("disableAutoTerminate=" + disableAutoTerminate);

        switch (command) {
            case "standalone":
                SmokeTestDriver.main(args);
                break;
            case "run":
                // this starts the driver (data generation and result verification)
                final int numKeys = 10;
                final int maxRecordsPerKey = 500;
                if (disableAutoTerminate) {
                    SmokeTestDriver.generate(kafka, numKeys, maxRecordsPerKey, false);
                } else {
                    Map<String, Set<Integer>> allData = SmokeTestDriver.generate(kafka, numKeys, maxRecordsPerKey);
                    SmokeTestDriver.verify(kafka, allData, maxRecordsPerKey);
                }
                break;
            case "process":
                // this starts a KafkaStreams client
                final SmokeTestClient client = new SmokeTestClient(streamsProperties);
                client.start();
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
