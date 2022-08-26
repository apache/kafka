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

import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.shell.MetadataNode.DirectoryNode;
import org.apache.kafka.shell.MetadataNode.FileNode;
import org.jline.reader.Candidate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Implements the cat command.
 */
public final class CatCommandHandler implements Commands.Handler {
    private static final Logger log = LoggerFactory.getLogger(CatCommandHandler.class);

    public final static Commands.Type TYPE = new CatCommandType();

    public static class CatCommandType implements Commands.Type {
        private CatCommandType() {
        }

        @Override
        public String name() {
            return "cat";
        }

        @Override
        public String description() {
            return "Show the contents of metadata nodes.";
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
            return new CatCommandHandler(namespace.getList("targets"));
        }

        @Override
        public void completeNext(MetadataNodeManager nodeManager, List<String> nextWords,
                                 List<Candidate> candidates) throws Exception {
            CommandUtils.completePath(nodeManager, nextWords.get(nextWords.size() - 1),
                candidates);
        }
    }

    private final List<String> targets;

    public CatCommandHandler(List<String> targets) {
        this.targets = targets;
    }

    @Override
    public void run(Optional<InteractiveShell> shell,
                    PrintWriter writer,
                    MetadataNodeManager manager) throws Exception {
        log.trace("cat " + targets);
        for (String target : targets) {
            manager.visit(new GlobVisitor(target, entryOption -> {
                if (entryOption.isPresent()) {
                    MetadataNode node = entryOption.get().node();
                    if (node instanceof DirectoryNode) {
                        writer.println("cat: " + target + ": Is a directory");
                    } else if (node instanceof FileNode) {
                        FileNode fileNode = (FileNode) node;
                        writer.println(fileNode.contents());
                    }
                } else {
                    writer.println("cat: " + target + ": No such file or directory.");
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
        if (!(other instanceof CatCommandHandler)) return false;
        CatCommandHandler o = (CatCommandHandler) other;
        if (!Objects.equals(o.targets, targets)) return false;
        return true;
    }
}
