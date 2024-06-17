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

package org.apache.kafka.shell.command;

import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.shell.InteractiveShell;
import org.apache.kafka.shell.state.MetadataShellState;
import org.jline.reader.Candidate;

import java.io.PrintWriter;
import java.util.List;
import java.util.Optional;

/**
 * Implements the help command.
 */
public final class HelpCommandHandler implements Commands.Handler {
    public final static Commands.Type TYPE = new HelpCommandType();

    public static class HelpCommandType implements Commands.Type {
        private HelpCommandType() {
        }

        @Override
        public String name() {
            return "help";
        }

        @Override
        public String description() {
            return "Display this help message.";
        }

        @Override
        public boolean shellOnly() {
            return true;
        }

        @Override
        public void addArguments(ArgumentParser parser) {
            // nothing to do
        }

        @Override
        public Commands.Handler createHandler(Namespace namespace) {
            return new HelpCommandHandler();
        }

        @Override
        public void completeNext(
            MetadataShellState state,
            List<String> nextWords,
            List<Candidate> candidates
        ) throws Exception {
            // nothing to do
        }
    }

    @Override
    public void run(
        Optional<InteractiveShell> shell,
        PrintWriter writer,
        MetadataShellState state
    ) {
        writer.printf("Welcome to the Apache Kafka metadata shell.%n%n");
        new Commands(true).parser().printHelp(writer);
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof HelpCommandHandler)) return false;
        return true;
    }
}
