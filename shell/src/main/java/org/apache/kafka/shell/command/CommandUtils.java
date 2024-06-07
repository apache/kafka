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

import org.apache.kafka.image.node.MetadataNode;
import org.apache.kafka.shell.state.MetadataShellState;
import org.jline.reader.Candidate;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

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
        while (command != null && command.startsWith(commandPrefix)) {
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
        for (String component : components) {
            if (!component.isEmpty()) {
                results.add(component);
            }
        }
        return results;
    }

    public static List<String> stripDotPathComponents(List<String> input) {
        List<String> output = new ArrayList<>();
        for (String string : input) {
            if (string.equals("..")) {
                if (!output.isEmpty()) {
                    output.remove(output.size() - 1);
                }
            } else if (!string.equals(".")) {
                output.add(string);
            }
        }
        return output;
    }

    /**
     * Generate a list of potential completions for a path.
     *
     * @param state             The MetadataShellState.
     * @param pathPrefix        The path prefix.  Non-null.
     * @param candidates        The list to add the output completions to.
     */
    public static void completePath(
        MetadataShellState state,
        String pathPrefix,
        List<Candidate> candidates
    ) {
        state.visit(data -> {
            String absolutePath = pathPrefix.startsWith("/") ?
                pathPrefix : data.workingDirectory() + "/" + pathPrefix;
            List<String> pathComponents = stripDotPathComponents(splitPath(absolutePath));
            MetadataNode directory = data.root();
            int numDirectories = pathPrefix.endsWith("/") ?
                pathComponents.size() : pathComponents.size() - 1;
            for (int i = 0; i < numDirectories; i++) {
                MetadataNode node = directory.child(pathComponents.get(i));
                if (node == null || !node.isDirectory()) {
                    return;
                }
                directory = node;
            }
            String lastComponent = "";
            if (numDirectories >= 0 && numDirectories < pathComponents.size()) {
                lastComponent = pathComponents.get(numDirectories);
            }
            TreeSet<String> children = new TreeSet<>(directory.childNames());
            String candidate = children.ceiling(lastComponent);
            String effectivePrefix;
            int lastSlash = pathPrefix.lastIndexOf('/');
            if (lastSlash < 0) {
                effectivePrefix = "";
            } else {
                effectivePrefix = pathPrefix.substring(0, lastSlash + 1);
            }
            while (candidate != null && candidate.startsWith(lastComponent)) {
                StringBuilder candidateBuilder = new StringBuilder();
                candidateBuilder.append(effectivePrefix).append(candidate);
                boolean complete = true;
                MetadataNode child = directory.child(candidate);
                if (child != null && child.isDirectory()) {
                    candidateBuilder.append("/");
                    complete = false;
                }
                candidates.add(new Candidate(candidateBuilder.toString(),
                    candidateBuilder.toString(), null, null, null, null, complete));
                candidate = children.higher(candidate);
            }
        });
    }
}
