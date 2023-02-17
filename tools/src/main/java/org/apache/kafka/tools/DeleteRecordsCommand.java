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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import joptsimple.OptionSpec;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.server.util.CommandDefaultOptions;
import org.apache.kafka.server.util.CommandLineUtils;

import java.io.PrintStream;
import java.util.Collection;
import java.util.Objects;

/**
 * A command for delete records of the given partitions down to the specified offset.
 */
public class DeleteRecordsCommand {
    private static final int EarliestVersion = 1;

    public static void main(String[] args) {
        execute(args, System.out);
    }

    private Collection<Tuple<TopicPartition, Long>> parseOffsetJsonStringWithoutDedup(String jsonData) {
        try {
            ObjectMapper mapper = new ObjectMapper();

            JsonNode js = mapper.readTree(jsonData);
        } catch (JsonMappingException | JsonProcessingException e) {
            throw new AdminOperationException("The input string is not a valid JSON")
        }

    }

    public static void execute(String[] args, PrintStream out) {

    }

    private static class JmxToolOptions extends CommandDefaultOptions {
        private final OptionSpec<?> bootstrapServerOpt;
        private final OptionSpec<?> offsetJsonFileOpt;
        private final OptionSpec<?> commandConfigOpt;

        public JmxToolOptions(String[] args) {
            super(args);

            bootstrapServerOpt = parser.accepts("bootstrap-server", "REQUIRED: The server to connect to.")
                .withRequiredArg()
                .describedAs("server(s) to use for bootstrapping")
                .ofType(String.class);

            offsetJsonFileOpt = parser.accepts("offset-json-file", "REQUIRED: The JSON file with offset per partition. " +
                    "The format to use is:\n" +
                    "{\"partitions\":\n  [{\"topic\": \"foo\", \"partition\": 1, \"offset\": 1}],\n \"version\":1\n}")
                .withRequiredArg()
                .describedAs("Offset json file path")
                .ofType(String.class);

            commandConfigOpt = parser.accepts("command-config", "A property file containing configs to be passed to Admin Client.")
                .withRequiredArg()
                .describedAs("command config property file path")
                .ofType(String.class);

            options = parser.parse(args);

            CommandLineUtils.maybePrintHelpOrVersion(this, "This tool helps to delete records of the given partitions down to the specified offset.");

            CommandLineUtils.checkRequiredArgs(parser, options, bootstrapServerOpt, offsetJsonFileOpt);
        }
    }

    public static final class Tuple<V1, V2> {
        private final V1 v1;

        private final V2 v2;

        public Tuple(V1 v1, V2 v2) {
            this.v1 = v1;
            this.v2 = v2;
        }

        public V1 v1() {
            return v1;
        }

        public V2 v2() {
            return v2;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Tuple<?, ?> tuple = (Tuple<?, ?>) o;
            return Objects.equals(v1, tuple.v1) && Objects.equals(v2, tuple.v2);
        }

        @Override
        public int hashCode() {
            return Objects.hash(v1, v2);
        }

        @Override
        public String toString() {
            return "Tuple{" +
                "v1=" + v1 +
                ", v2=" + v2 +
                '}';
        }
    }
}
