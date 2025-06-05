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

import joptsimple.OptionParser;
import joptsimple.OptionSpec;

public class LogCompactionTesterOptions {
    public final OptionSpec<Long> numMessagesOpt;
    public final OptionSpec<String>  messageCompressionOpt;
    public final OptionSpec<Integer> numDupsOpt;
    public final OptionSpec<String>  brokerOpt;
    public final OptionSpec<Integer> topicsOpt;
    public final OptionSpec<Integer> percentDeletesOpt;
    public final OptionSpec<Integer> sleepSecsOpt;
    public final OptionSpec<Void>    helpOpt;

    public LogCompactionTesterOptions(OptionParser parser) {
        numMessagesOpt = parser
                .accepts("messages", "The number of messages to send or consume.")
                .withRequiredArg()
                .describedAs("count")
                .ofType(Long.class)
                .defaultsTo(Long.MAX_VALUE);
        messageCompressionOpt = parser
                .accepts("compression-type", "message compression type")
                .withOptionalArg()
                .describedAs("compressionType")
                .ofType(String.class)
                .defaultsTo("none");

        numDupsOpt = parser
                .accepts("duplicates", "The number of duplicates for each key.")
                .withRequiredArg()
                .describedAs("count")
                .ofType(Integer.class)
                .defaultsTo(5);

        brokerOpt = parser
                .accepts("bootstrap-server", "The server(s) to connect to.")
                .withRequiredArg()
                .describedAs("url")
                .ofType(String.class);

        topicsOpt = parser
                .accepts("topics", "The number of topics to test.")
                .withRequiredArg()
                .describedAs("count")
                .ofType(Integer.class)
                .defaultsTo(1);

        percentDeletesOpt = parser
                .accepts("percent-deletes", "The percentage of updates that are deletes.")
                .withRequiredArg()
                .describedAs("percent")
                .ofType(Integer.class)
                .defaultsTo(0);

        sleepSecsOpt = parser
                .accepts("sleep", "Time in milliseconds to sleep between production and consumption.")
                .withRequiredArg()
                .describedAs("ms")
                .ofType(Integer.class)
                .defaultsTo(0);

        helpOpt = parser
                .acceptsAll(java.util.Arrays.asList("h", "help"), "Display help information");
    }
}
