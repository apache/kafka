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
import org.jline.reader.Candidate;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility functions for command handlers.
 */
public final class CommandUtils {
    /**
     * Convert a list of paths into the effective list of paths which should be used.
     * Empty strings will be removed.  If no paths are given, the current working
     * directory will be used.
     *
     * @param paths     The input paths.  Non-null.
     *
     * @return          The output paths.
     */
    public static List<String> getEffectivePaths(List<String> paths) {
        List<String> effectivePaths = new ArrayList<>();
        for (String path : paths) {
            if (!path.isEmpty()) {
                effectivePaths.add(path);
            }
        }
        if (effectivePaths.isEmpty()) {
            effectivePaths.add(".");
        }
        return effectivePaths;
    }

    /**
     * Generate a list of potential completions for a prefix of a command name.
     *
     * @param commandPrefix     The command prefix.  Non-null.
     * @param candidates        The list to add the output completions to.
     */
    public static void completeCommand(String commandPrefix, List<Candidate> candidates) {
        String command = Commands.TYPES.ceilingKey(commandPrefix);
        while (true) {
            if (command == null || !command.startsWith(commandPrefix)) {
                return;
            }
            candidates.add(new Candidate(command));
            command = Commands.TYPES.higherKey(command);
        }
    }

    /**
     * Convert a path to a list of path components.
     * Multiple slashes in a row are treated the same as a single slash.
     * Trailing slashes are ignored.
     */
    public static List<String> splitPath(String path) {
        List<String> results = new ArrayList<>();
        String[] components = path.split("/");
        for (int i = 0; i < components.length; i++) {
            if (!components[i].isEmpty()) {
                results.add(components[i]);
            }
        }
        return results;
    }

    /**
     * Generate a list of potential completions for a path.
     *
     * @param nodeManager       The NodeManager.
     * @param pathPrefix        The path prefix.  Non-null.
     * @param candidates        The list to add the output completions to.
     */
    public static void completePath(MetadataNodeManager nodeManager,
                                    String pathPrefix,
                                    List<Candidate> candidates) throws Exception {
        nodeManager.visit(data -> {
            String effectivePath = pathPrefix.startsWith("/") ?
                data.workingDirectory() + "/" + pathPrefix : "./" + pathPrefix;
            List<String> pathComponents = splitPath(effectivePath);
            DirectoryNode directory = data.root();
            for (int i = 0; i < pathComponents.size() - 1; i++) {
                MetadataNode node = directory.child(pathComponents.get(i));
                if (node == null || !(node instanceof DirectoryNode)) {
                    return;
                }
                directory = (DirectoryNode) node;
            }
            String lastComponent = pathComponents.size() == 0 ? "" :
                pathComponents.get(pathComponents.size() - 1);
            String candidate = directory.children().ceilingKey(lastComponent);
            while (candidate != null && candidate.startsWith(lastComponent)) {
                candidates.add(new Candidate(candidate));
                candidate = directory.children().higherKey(candidate);
            }
        });
    }
}
