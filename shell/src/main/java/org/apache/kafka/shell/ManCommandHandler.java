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

package org.apache.kafka.shell;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;
import org.jline.reader.Candidate;

import java.io.PrintWriter;
import java.util.List;
import java.util.Optional;

/**
 * Implements the manual command.
 */
public final class ManCommandHandler implements Commands.Handler {
    private final String cmd;

    public final static Commands.Type TYPE = new ManCommandType();

    public static class ManCommandType implements Commands.Type {
        private ManCommandType() {
        }

        @Override
        public String name() {
            return "man";
        }

        @Override
        public String description() {
            return "Show the help text for a specific command.";
        }

        @Override
        public boolean shellOnly() {
            return true;
        }

        @Override
        public void addArguments(ArgumentParser parser) {
            parser.addArgument("cmd").
                nargs(1).
                help("The command to get help text for.");
        }

        @Override
        public Commands.Handler createHandler(Namespace namespace) {
            return new ManCommandHandler(namespace.<String>getList("cmd").get(0));
        }

        @Override
        public void completeNext(MetadataNodeManager nodeManager, List<String> nextWords,
                                 List<Candidate> candidates) throws Exception {
            if (nextWords.size() == 1) {
                CommandUtils.completeCommand(nextWords.get(0), candidates);
            }
        }
    }

    public ManCommandHandler(String cmd) {
        this.cmd = cmd;
    }

    @Override
    public void run(Optional<InteractiveShell> shell,
                    PrintWriter writer,
                    MetadataNodeManager manager) {
        Commands.Type type = Commands.TYPES.get(cmd);
        if (type == null) {
            writer.println("man: unknown command " + cmd +
                ". Type help to get a list of commands.");
        } else {
            ArgumentParser parser = ArgumentParsers.newArgumentParser(type.name(), false);
            type.addArguments(parser);
            writer.printf("%s: %s%n%n", cmd, type.description());
            parser.printHelp(writer);
        }
    }

    @Override
    public int hashCode() {
        return cmd.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ManCommandHandler)) return false;
        ManCommandHandler o = (ManCommandHandler) other;
        if (!o.cmd.equals(cmd)) return false;
        return true;
    }
}
