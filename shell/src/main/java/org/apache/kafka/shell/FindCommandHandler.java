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
import org.jline.reader.Candidate;

import java.io.PrintWriter;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;

/**
 * Implements the find command.
 */
public final class FindCommandHandler implements Commands.Handler {
    public final static Commands.Type TYPE = new FindCommandType();

    public static class FindCommandType implements Commands.Type {
        private FindCommandType() {
        }

        @Override
        public String name() {
            return "find";
        }

        @Override
        public String description() {
            return "Search for nodes in the directory hierarchy.";
        }

        @Override
        public boolean shellOnly() {
            return false;
        }

        @Override
        public void addArguments(ArgumentParser parser) {
            parser.addArgument("paths").
                nargs("*").
                help("The paths to start at.");
        }

        @Override
        public Commands.Handler createHandler(Namespace namespace) {
            return new FindCommandHandler(namespace.getList("paths"));
        }

        @Override
        public void completeNext(MetadataNodeManager nodeManager, List<String> nextWords,
                                 List<Candidate> candidates) throws Exception {
            CommandUtils.completePath(nodeManager, nextWords.get(nextWords.size() - 1),
                candidates);
        }
    }

    private final List<String> paths;

    public FindCommandHandler(List<String> paths) {
        this.paths = paths;
    }

    @Override
    public void run(Optional<InteractiveShell> shell,
                    PrintWriter writer,
                    MetadataNodeManager manager) throws Exception {
        for (String path : CommandUtils.getEffectivePaths(paths)) {
            manager.visit(new GlobVisitor(path, entryOption -> {
                if (entryOption.isPresent()) {
                    find(writer, path, entryOption.get().node());
                } else {
                    writer.println("find: " + path + ": no such file or directory.");
                }
            }));
        }
    }

    private void find(PrintWriter writer, String path, MetadataNode node) {
        writer.println(path);
        if (node instanceof DirectoryNode) {
            DirectoryNode directory = (DirectoryNode) node;
            for (Entry<String, MetadataNode> entry : directory.children().entrySet()) {
                String nextPath = path.equals("/") ?
                    path + entry.getKey() : path + "/" + entry.getKey();
                find(writer, nextPath, entry.getValue());
            }
        }
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(paths);
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof FindCommandHandler)) return false;
        FindCommandHandler o = (FindCommandHandler) other;
        if (!Objects.equals(o.paths, paths)) return false;
        return true;
    }
}
