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

import java.io.PrintWriter;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Implements the cd command.
 */
public final class CdCommandHandler implements Command.Handler {
    private final Optional<String> target;

    public CdCommandHandler(Optional<String> target) {
        this.target = target;
    }

    @Override
    public void run(Optional<MetadataShell> shell,
                    PrintWriter writer,
                    MetadataNodeManager manager) throws Exception {
        String effectiveTarget = target.orElse("/");
        manager.visit(new Consumer<MetadataNodeManager.Data>() {
            @Override
            public void accept(MetadataNodeManager.Data data) {
                new GlobVisitor(effectiveTarget, entryOption -> {
                    if (entryOption.isPresent()) {
                        if (!(entryOption.get().node() instanceof DirectoryNode)) {
                            writer.println("cd: " + effectiveTarget + ": not a directory.");
                        } else {
                            String[] path = entryOption.get().path();
                            data.setWorkingDirectory("/" + String.join("/", path));
                        }
                    } else {
                        writer.println("cd: " + effectiveTarget + ": no such directory.");
                    }
                }).accept(data);
            }
        });
    }

    @Override
    public int hashCode() {
        return target.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof CdCommandHandler)) return false;
        CdCommandHandler o = (CdCommandHandler) other;
        if (!o.target.equals(target)) return false;
        return true;
    }
}
