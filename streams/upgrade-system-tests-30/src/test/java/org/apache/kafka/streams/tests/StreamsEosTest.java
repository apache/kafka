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

import java.io.IOException;
import java.util.Properties;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamsConfig;

public class StreamsEosTest {

    /**
     *  args ::= kafka propFileName command
     *  command := "process" | "process-complex"
     */
    @SuppressWarnings("deprecation")
    public static void main(final String[] args) throws IOException {
        if (args.length < 2) {
            System.err.println("StreamsEosTest are expecting two parameters: propFile, command; but only see " + args.length + " parameter");
            Exit.exit(1);
        }

        final String propFileName = args[0];
        final String command = args[1];

        final Properties streamsProperties = Utils.loadProps(propFileName);
        final String kafka = streamsProperties.getProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG);

        if (kafka == null) {
            System.err.println("No bootstrap kafka servers specified in " + StreamsConfig.BOOTSTRAP_SERVERS_CONFIG);
            Exit.exit(1);
        }

        System.out.println("StreamsTest instance started");
        System.out.println("kafka=" + kafka);
        System.out.println("props=" + streamsProperties);
        System.out.println("command=" + command);
        System.out.flush();

        if (command == null || propFileName == null) {
            Exit.exit(-1);
        }

        switch (command) {
            case "process":
                new EosTestClient(streamsProperties, false).start();
                break;
            case "process-complex":
                new EosTestClient(streamsProperties, true).start();
                break;
            default:
                System.out.println("unknown command: " + command);
                System.out.flush();
                Exit.exit(-1);
        }
    }

}
