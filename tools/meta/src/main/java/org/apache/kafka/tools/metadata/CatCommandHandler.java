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

package org.apache.kafka.tools.metadata;

import org.apache.kafka.tools.metadata.MetadataNode.DirectoryNode;
import org.apache.kafka.tools.metadata.MetadataNode.FileNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.util.List;
import java.util.Optional;

/**
 * Implements the cat command.
 */
public final class CatCommandHandler implements Command.Handler {
    private static final Logger log = LoggerFactory.getLogger(CatCommandHandler.class);

    private final List<String> targets;

    public CatCommandHandler(List<String> targets) {
        this.targets = targets;
    }

    @Override
    public void run(Optional<MetadataShell> shell,
                    PrintWriter writer,
                    MetadataNodeManager manager) throws Exception {
        log.trace("cat " + targets);
        for (String target : targets) {
            manager.visit(new GlobVisitor(target, entryOption -> {
                if (entryOption.isPresent()) {
                    MetadataNode node = entryOption.get().getValue();
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
}
