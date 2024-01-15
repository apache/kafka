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
import org.apache.kafka.image.node.MetadataNode;
import org.apache.kafka.shell.InteractiveShell;
import org.apache.kafka.shell.glob.GlobVisitor;
import org.apache.kafka.shell.node.printer.ShellNodePrinter;
import org.apache.kafka.shell.state.MetadataShellState;
import org.jline.reader.Candidate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Implements the tree command.
 */
public final class TreeCommandHandler implements Commands.Handler {
    private static final Logger log = LoggerFactory.getLogger(TreeCommandHandler.class);

    public final static Commands.Type TYPE = new CatCommandType();

    public static class CatCommandType implements Commands.Type {
        private CatCommandType() {
        }

        @Override
        public String name() {
            return "tree";
        }

        @Override
        public String description() {
            return "Show the contents of metadata nodes in a tree structure.";
        }

        @Override
        public boolean shellOnly() {
            return false;
        }

        @Override
        public void addArguments(ArgumentParser parser) {
            parser.addArgument("targets").
                nargs("+").
                help("The metadata nodes to display.");
        }

        @Override
        public Commands.Handler createHandler(Namespace namespace) {
            return new TreeCommandHandler(namespace.getList("targets"));
        }

        @Override
        public void completeNext(
            MetadataShellState state,
            List<String> nextWords,
            List<Candidate> candidates
        ) throws Exception {
            CommandUtils.completePath(state, nextWords.get(nextWords.size() - 1), candidates);
        }
    }

    private final List<String> targets;

    public TreeCommandHandler(List<String> targets) {
        this.targets = targets;
    }

    @Override
    public void run(
        Optional<InteractiveShell> shell,
        PrintWriter writer,
        MetadataShellState state
    ) throws Exception {
        log.trace("tree " + targets);
        for (String target : targets) {
            state.visit(new GlobVisitor(target, entryOption -> {
                if (entryOption.isPresent()) {
                    MetadataNode node = entryOption.get().node();
                    ShellNodePrinter printer = new ShellNodePrinter(writer);
                    node.print(printer);
                } else {
                    writer.println("tree: " + target + ": No such file or directory.");
                }
            }));
        }
    }

    @Override
    public int hashCode() {
        return targets.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof TreeCommandHandler)) return false;
        TreeCommandHandler o = (TreeCommandHandler) other;
        if (!Objects.equals(o.targets, targets)) return false;
        return true;
    }
}
