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

import java.io.File;

public class StreamsEosTest {

    /**
     *  args ::= command kafka zookeeper stateDir
     *  command := "run" | "process" | "verify"
     */
    public static void main(final String[] args) throws Exception {
        final String kafka = args[0];
        final String stateDir = args.length > 1 ? args[1] : null;
        final String command = args.length > 2 ? args[2] : null;

        System.out.println("StreamsTest instance started");
        System.out.println("command=" + command);
        System.out.println("kafka=" + kafka);
        System.out.println("stateDir=" + stateDir);

        if (command == null || stateDir == null) {
            System.exit(-1);
        }

        switch (command) {
            case "run":
                EosTestDriver.generate(kafka);
                break;
            case "process":
                final EosTestClient client = new EosTestClient(new File(stateDir), kafka);
                client.start();
                break;
            case "verify":
                EosTestDriver.verify(kafka);
                break;
            default:
                System.out.println("unknown command: " + command);
        }
    }

}
