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

package org.apache.kafka.message.checker;

import org.apache.kafka.message.FieldSpec;
import org.apache.kafka.message.MessageGenerator;
import org.apache.kafka.message.MessageSpec;
import org.apache.kafka.message.Versions;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectLoader;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevTree;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.treewalk.TreeWalk;
import org.eclipse.jgit.treewalk.filter.PathFilter;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Utilities for the metadata schema checker.
 */
class CheckerUtils {
    /**
     * A min function defined for shorts.
     *
     * @param a     The first short integer to compare.
     * @param b     The second short integer to compare.
     * @return      The minimum short integer.
     */
    static short min(short a, short b) {
        return a < b ? a : b;
    }

    /**
     * A max function defined for shorts.
     *
     * @param a     The first short integer to compare.
     * @param b     The second short integer to compare.
     * @return      The maximum short integer.
     */
    static short max(short a, short b) {
        return a > b ? a : b;
    }

    /**
     * Validate the a field doesn't have tagged versions that are outside of the top-level flexible
     * versions.
     *
     * @param what                      A description of the field.
     * @param field                     The field to validate.
     * @param topLevelFlexibleVersions  The top-level flexible versions.
     */
    static void validateTaggedVersions(
        String what,
        FieldSpec field,
        Versions topLevelFlexibleVersions
    ) {
        if (!field.flexibleVersions().isPresent()) {
            if (!topLevelFlexibleVersions.contains(field.taggedVersions())) {
                throw new RuntimeException("Tagged versions for " + what + " " +
                        field.name() + " are " + field.taggedVersions() + ", but top " +
                        "level flexible versions are " + topLevelFlexibleVersions);
            }
        }
    }

    /**
     * Read a MessageSpec file from a path.
     *
     * @param schemaPath    The path to read the file from.
     * @return              The MessageSpec.
     */
    static MessageSpec readMessageSpecFromFile(String schemaPath) {
        if (!Files.isReadable(Paths.get(schemaPath))) {
            throw new RuntimeException("Path " + schemaPath + " does not point to " +
                    "a readable file.");
        }
        try {
            return MessageGenerator.JSON_SERDE.readValue(new File(schemaPath), MessageSpec.class);
        } catch (Exception e) {
            throw new RuntimeException("Unable to parse file as MessageSpec: " + schemaPath, e);
        }
    }

    /**
     * Return a MessageSpec file give file contents.
     *
     * @param contents      The path to read the file from.
     * @return              The MessageSpec.
     */
    static MessageSpec readMessageSpecFromString(String contents) {
        try {
            return MessageGenerator.JSON_SERDE.readValue(contents, MessageSpec.class);
        } catch (Exception e) {
            throw new RuntimeException("Unable to parse string as MessageSpec: " + contents, e);
        }
    }

    /**
     * Read a MessageSpec file from remote git repo.
     *
     * @param filePath The file to read from remote git repo.
     * @param ref The specific git reference to be used for testing.
     * @return The file contents.
     */
    static String getDataFromGit(String filePath, Path gitPath, String ref) throws IOException {
        Git git = Git.open(new File(gitPath + "/.git"));
        Repository repository = git.getRepository();
        Ref head = repository.getRefDatabase().findRef(ref);
        if (head == null) {
            throw new IllegalStateException("Cannot find " + ref + " in the repository.");
        }

        try (RevWalk revWalk = new RevWalk(repository)) {
            RevCommit commit = revWalk.parseCommit(head.getObjectId());
            RevTree tree = commit.getTree();
            try (TreeWalk treeWalk = new TreeWalk(repository)) {
                treeWalk.addTree(tree);
                treeWalk.setRecursive(true);
                treeWalk.setFilter(PathFilter.create(String.valueOf(Paths.get(filePath.substring(1)))));
                if (!treeWalk.next()) {
                    throw new IllegalStateException("Did not find expected file " + filePath.substring(1));
                }
                ObjectId objectId = treeWalk.getObjectId(0);
                ObjectLoader loader = repository.open(objectId);
                return new String(loader.getBytes(), StandardCharsets.UTF_8);
            }
        }
    }
}
