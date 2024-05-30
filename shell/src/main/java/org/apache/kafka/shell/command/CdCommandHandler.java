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
import org.apache.kafka.shell.glob.GlobVisitor;
import org.apache.kafka.shell.state.MetadataShellState;
import org.jline.reader.Candidate;

import java.io.PrintWriter;
import java.util.List;
import java.util.Optional;

/**
 * Implements the cd command.
 */
public final class CdCommandHandler implements Commands.Handler {
    public final static Commands.Type TYPE = new CdCommandType();

    public static class CdCommandType implements Commands.Type {
        private CdCommandType() {
        }

        @Override
        public String name() {
            return "cd";
        }

        @Override
        public String description() {
            return "Set the current working directory.";
        }

        @Override
        public boolean shellOnly() {
            return true;
        }

        @Override
        public void addArguments(ArgumentParser parser) {
            parser.addArgument("target").
                nargs("?").
                help("The directory to change to.");
        }

        @Override
        public Commands.Handler createHandler(Namespace namespace) {
            return new CdCommandHandler(Optional.ofNullable(namespace.getString("target")));
        }

        @Override
        public void completeNext(
            MetadataShellState state,
            List<String> nextWords,
            List<Candidate> candidates
        ) throws Exception {
            if (nextWords.size() == 1) {
                CommandUtils.completePath(state, nextWords.get(0), candidates);
            }
        }
    }

    private final Optional<String> target;

    public CdCommandHandler(Optional<String> target) {
        this.target = target;
    }

    @Override
    public void run(
        Optional<InteractiveShell> shell,
        PrintWriter writer,
        MetadataShellState state
    ) throws Exception {
        String effectiveTarget = target.orElse("/");
        new GlobVisitor(effectiveTarget, entryOption -> {
            if (entryOption.isPresent()) {
                if (!entryOption.get().node().isDirectory()) {
                    writer.println("cd: " + effectiveTarget + ": not a directory.");
                } else {
                    state.setWorkingDirectory(entryOption.get().absolutePath());
                }
            } else {
                writer.println("cd: " + effectiveTarget + ": no such directory.");
            }
        }).accept(state);
    }

    @Override
    public int hashCode() {
        return target.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof CdCommandHandler)) return false;
        return ((CdCommandHandler) other).target.equals(target);
    }
}
