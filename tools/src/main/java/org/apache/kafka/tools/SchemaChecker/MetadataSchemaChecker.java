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

package org.apache.kafka.tools.SchemaChecker;



import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.kafka.clients.admin.FinalizedVersionRange;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectLoader;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevTree;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.treewalk.TreeWalk;
import org.eclipse.jgit.treewalk.filter.PathFilter;
import org.eclipse.jgit.treewalk.filter.TreeFilter;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;


public class MetadataSchemaChecker {

    static int latestTag = -1;
    static int  latestTagVersion = -1;
    static int originalHighestVersion = -1;
    static int originalLowestVersion = -1;
    static int editedHighestVersion = -1;
    static int editedLowestVersion = -1;
    static String[] filesCheckMetadata = Paths.get(Paths.get("").toAbsolutePath() + "/metadata/src/main/resources/common/metadata/").toFile().list();
    public static void main(String[] args) throws Exception {

        try {
            List<String> localContent = new ArrayList<>();
            for (String fileName: filesCheckMetadata) {
                final String dir = String.valueOf(Paths.get("").toAbsolutePath());
                Path path = Paths.get(dir + "/metadata/src/main/resources/common/metadata/" + fileName);
                BufferedReader reader = new BufferedReader(new FileReader(String.valueOf(path)));
                StringBuilder content = new StringBuilder();
                boolean print = false;
                for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                    if(!line.isEmpty()) {
                        if (line.charAt(0) == '{') {
                            print = true;
                        }
                        if (print && !line.trim().startsWith("//")) {
                            content.append(line);
                        }
                    }
                }
                localContent.add(content.toString());
            }

            List<String> gitContent = GetDataFromGit();
            if (localContent.size() != gitContent.size()) {
                throw new IllegalStateException("missing schemas");
            }
            for(int i = 0; i < localContent.size(); i++) {
                if (Objects.equals(localContent.get(i), gitContent.get(i))) {
                    continue;
                }

                ObjectMapper objectMapper = new ObjectMapper();
                JsonNode jsonNode1 = objectMapper.readTree(gitContent.get(i));
                JsonNode jsonNode2 = objectMapper.readTree(localContent.get(i));

                checkApiTypeVersions(jsonNode1, jsonNode2);
                parser((ArrayNode) jsonNode1.get("fields"), (ArrayNode) jsonNode2.get("fields"));
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static List<String> GetDataFromGit() throws IOException, GitAPIException {
        List<String> gitSchemas = new ArrayList<>();

        Git git = Git.open(new File(Paths.get("").toAbsolutePath() + "/.git"));
        Repository repository = git.getRepository();
        Ref head = git.getRepository().getRefDatabase().firstExactRef("refs/heads/trunk");

        try (RevWalk revWalk = new RevWalk(repository)) {
            RevCommit commit = revWalk.parseCommit(head.getObjectId());
            RevTree tree = commit.getTree();
            for (String fileName : filesCheckMetadata) {
                StringBuilder stringBuilder = new StringBuilder();
                try (TreeWalk treeWalk = new TreeWalk(repository)) {
                    treeWalk.addTree(tree);
                    treeWalk.setRecursive(true);
                    treeWalk.setFilter(PathFilter.create(String.valueOf(Paths.get("metadata/src/main/resources/common/metadata/" + fileName))));
                    if (!treeWalk.next()) {
                        throw new IllegalStateException("Did not find expected file /metadata/src/main/resources/common/metadata/" + fileName);
                    }
                    ObjectId objectId = treeWalk.getObjectId(0);
                    ObjectLoader loader = repository.open(objectId);

                    String content = new String(loader.getBytes());
                    String[] lines = content.split("\n");
                    boolean print = false;
                    for (String line : lines) {
                        if(!line.isEmpty()) {
                            if (line.charAt(0) == '{') {
                                print = true;
                            }
                            if (print && !line.trim().startsWith("//")) {
                                stringBuilder.append(line);
                            }
                        }
                    }
                }
                gitSchemas.add(stringBuilder.toString());
            }
            revWalk.dispose();
        }
        return gitSchemas;
    }

    private static void checkApiTypeVersions(JsonNode original, JsonNode edited) {
        if (!Objects.equals(original.get("apiKey"), edited.get("apiKey"))) {
            throw new IllegalStateException("New schema has wrong api key, Original api key: " + edited.get("apiKey") +
                                            ", New api key: " + original.get("apiKey"));
        }
        if (!Objects.equals(original.get("type"), edited.get("type"))) {
            throw new IllegalStateException("New schema has wrong record type, Original type: " + edited.get("type") +
                                            ", New Type: " + original.get("type"));
        }
        String[] oldValidVersions = cleanUpVersionStrings(String.valueOf(original.get("validVersions")));
        String[] newValidVersions = cleanUpVersionStrings(String.valueOf(edited.get("validVersions")));

        originalLowestVersion = Integer.parseInt(oldValidVersions[0]);
        editedLowestVersion = Integer.parseInt(newValidVersions[0]);
        if (oldValidVersions.length == 1) {
            originalHighestVersion = Integer.parseInt(oldValidVersions[0]);
        } else {
            originalHighestVersion = Integer.parseInt(oldValidVersions[1]);
        }
        if (newValidVersions.length == 1) {
            editedHighestVersion = Integer.parseInt(newValidVersions[0]);
        } else {
            editedHighestVersion = Integer.parseInt(newValidVersions[1]);
        }

        if (originalHighestVersion != editedHighestVersion && originalHighestVersion + 1 != editedHighestVersion) {
            throw new IllegalStateException("Invalid latest version, can at most be one higher than the previous. Original version: "
                                            + originalHighestVersion + ", New Version: " + editedHighestVersion);
        }
        if (originalLowestVersion != editedLowestVersion) {
            throw new IllegalStateException("cannot change lower end of valid versions");
        }
    }

    private static void parser(ArrayNode fieldsOrig, ArrayNode fieldsNew) {
        Iterator<JsonNode> iterNewNode = fieldsNew.iterator();
        Iterator<JsonNode> iterOldNode = fieldsOrig.iterator();
        boolean isNewField = false;
        JsonNode nodeOrig = null;
        while (iterOldNode.hasNext() || isNewField) {
            if (!isNewField) {
                nodeOrig = iterOldNode.next();
            }
            isNewField = false;

            if (!iterNewNode.hasNext()) {
                throw new IllegalStateException("New schema is missing field ");
            }
            JsonNode nodeNew = iterNewNode.next();

            if (checkTaggedFieldIfTag(nodeOrig, nodeNew)) {
                isNewField = true;
                continue;
            }

            if (parseVersion(nodeOrig, nodeNew)) {
                isNewField = true;
                continue;
            }

            if (!Objects.equals(nodeOrig.get("type"), nodeNew.get("type"))) {
                throw new IllegalStateException("Fields must have same type. Expected: " + nodeOrig.get("type") + " Received: " + nodeNew.get("type"));
            }

            parseNullableVersion(nodeOrig, nodeNew);


            if (nodeOrig.has("fields")) {
                while(!nodeNew.has("fields")) {
                    parseNewField(nodeNew);
                    nodeNew = iterNewNode.next();
                }
                parser((ArrayNode) nodeOrig.get("fields"), (ArrayNode) nodeNew.get("fields"));
            }
        }

        while (iterNewNode.hasNext()) {
            parseNewField(iterNewNode.next());
        }
    }

    private static boolean parseVersion(JsonNode nodeOrig, JsonNode nodeNew) {
        String[] oldVersion = cleanUpVersionStrings(String.valueOf(nodeOrig.get("versions")));
        String[] newVersion = cleanUpVersionStrings(String.valueOf(nodeNew.get("versions")));
        FinalizedVersionRange oldRange;
        FinalizedVersionRange newRange;

        if (oldVersion.length == 1) {
            oldRange = new FinalizedVersionRange(Short.parseShort(oldVersion[0]), Short.MAX_VALUE);
        } else {
            oldRange = new FinalizedVersionRange(Short.parseShort(oldVersion[0]), Short.parseShort(oldVersion[1]));
        }
        if (newVersion.length == 1) {
            newRange = new FinalizedVersionRange(Short.parseShort(newVersion[0]), Short.MAX_VALUE);
        } else {
            newRange = new FinalizedVersionRange(Short.parseShort(newVersion[0]), Short.parseShort(newVersion[1]));
        }

        if (!oldRange.equals(newRange)) {
            if (oldRange.maxVersionLevel() != Short.MAX_VALUE && newRange.maxVersionLevel() == Short.MAX_VALUE) {
                int cutoff = oldRange.maxVersionLevel();
                if (cutoff > newRange.minVersionLevel()) {
                    parseNewField(nodeNew);
                    return true;
                } else {
                    throw new IllegalStateException("new schema cannot reopen already closed field");
                }
            } else if (oldRange.maxVersionLevel() != Short.MAX_VALUE && newRange.maxVersionLevel() != Short.MAX_VALUE) {
                throw new IllegalStateException("cannot changed already closed field");
            } else if (oldRange.maxVersionLevel() == Short.MAX_VALUE && newRange.maxVersionLevel() != Short.MAX_VALUE) {
                if (oldRange.minVersionLevel() != newRange.minVersionLevel()) {
                    throw new IllegalStateException("cannot change lower end of ");
                }
                int cutoffVersion = newRange.maxVersionLevel();
                if (cutoffVersion != editedHighestVersion && cutoffVersion + 1 != editedHighestVersion) {
                    throw new IllegalStateException("Invalid closing version for field");
                }
            } else if (oldRange.maxVersionLevel() == Short.MAX_VALUE && newRange.maxVersionLevel() == Short.MAX_VALUE) {
                int oldInt = oldRange.minVersionLevel();
                int newInt = newRange.minVersionLevel();
                if (oldInt < newInt) {
                    parseNewField(nodeNew);
                    return true;
                } else {
                    throw new IllegalStateException("new field needs to be on a new version");
                }
            }
        }
        return false;
    }

    private static void parseNullableVersion(JsonNode nodeOrig, JsonNode nodeNew) {
        String[] oldVersion = cleanUpVersionStrings(String.valueOf(nodeOrig.get("nullableVersions")));
        String[] newVersion = cleanUpVersionStrings(String.valueOf(nodeNew.get("nullableVersions")));
        FinalizedVersionRange oldRange;
        FinalizedVersionRange newRange;
        if (oldVersion.length == 1) {
            oldRange = new FinalizedVersionRange(Short.parseShort(oldVersion[0]), Short.MAX_VALUE);
        } else {
            oldRange = new FinalizedVersionRange(Short.parseShort(oldVersion[0]), Short.parseShort(oldVersion[1]));
        }
        if (newVersion.length == 1) {
            newRange = new FinalizedVersionRange(Short.parseShort(newVersion[0]), Short.MAX_VALUE);
        } else {
            newRange = new FinalizedVersionRange(Short.parseShort(newVersion[0]), Short.parseShort(newVersion[1]));
        }

        if (nodeOrig.has("nullableVersions")) {
            if (!nodeNew.has("nullableVersions")) {
                throw new IllegalStateException("field is missing nullable information");
            }
            if (oldRange.maxVersionLevel() != Short.MAX_VALUE && newRange.maxVersionLevel() == Short.MAX_VALUE) {
                throw new IllegalStateException("cannot make field nullable after closing nullable versions");
            }
            if (oldRange.maxVersionLevel() == Short.MAX_VALUE && newRange.maxVersionLevel() != Short.MAX_VALUE) {

                if (oldRange.minVersionLevel() != newRange.minVersionLevel()) {
                    throw new IllegalStateException("invalid closing version for nullable versions");
                }
                if (newRange.maxVersionLevel() != editedHighestVersion) {
                    throw new IllegalStateException("incorrect closing version for nullable versions");
                }
            }

        } else if (nodeNew.has("nullableVersions")) {
            if (newRange.minVersionLevel() != editedHighestVersion) {
                throw new IllegalStateException("invalid version for new nullable version");
            }
        }
    }

    private static boolean checkTaggedFieldIfTag(JsonNode origNode, JsonNode newNode) {
        if (origNode.has("tag")) {
            if (!newNode.has("tag")) {
                parseNewField(newNode);
                return true;
            }
            if (!newNode.has("taggedVersions")) {
                throw new IllegalStateException("new schema tagged field is missing tagged version");
            }


            if (latestTag + 1 != Integer.parseInt(newNode.get("tag").asText())) {
                throw new IllegalStateException(" tag from new schema not in numeric order, " + newNode.get("tag").asText()
                                           + " should be "  + (latestTag + 1));
            }
            latestTag = Integer.parseInt(newNode.get("tag").asText());
            String[] x = cleanUpVersionStrings(origNode.get("taggedVersions").asText());
            latestTagVersion = Integer.parseInt(cleanUpVersionStrings(origNode.get("taggedVersions").asText())[0]);
        } else {
            if (newNode.has("tag")) {
                parseNewField(newNode);
            }
        }
        return false;
    }

    private static void parseNewField(JsonNode node) {
        String[] versions = cleanUpVersionStrings(String.valueOf(node.get("versions")));
        if (originalHighestVersion + 1 != editedHighestVersion && !node.has("tag")) {
            throw new IllegalStateException("New schemas with new fields need to be on the next version iteration");
        }
        if (editedHighestVersion != Integer.parseInt(versions[0]) && !node.has("tag")) {
            throw new IllegalStateException("new field version not correct");
        }
        if (!node.has("type") && !node.has("fields")) {
            throw new IllegalStateException("new field requires a type if it doesn't have fields node ");
        }

        if (Integer.parseInt(versions[0]) != (originalHighestVersion + 1) && !node.has("tag")) {
            throw new IllegalStateException("New field must be on next version");
        }
        if (node.has("tag")) {
            if (!node.has("taggedVersions")) {
                throw new IllegalStateException("new tagged field is missing tagged versions");
            }
            String[] taggedVersions = cleanUpVersionStrings(String.valueOf(node.get("taggedVersions")));

            if (Integer.parseInt(taggedVersions[0]) != latestTagVersion + 1) {
                throw new IllegalStateException("taggedVersion incorrect for new tagged field");
            }
            if (Integer.parseInt(versions[0]) < originalHighestVersion ||
                Integer.parseInt(versions[0]) > editedHighestVersion) {
                throw new IllegalStateException("Invalid version for new tagged field");
            }
        }
    }

    private static String[] cleanUpVersionStrings(String string) {
        String[] cleanedInts;
        if (string.charAt(0) == '"') {
            string = string.substring(1, string.length() - 1);
        }
        if (string.contains("-")) {
            cleanedInts = string.split("-");
        } else {
            cleanedInts = string.split("\\+");
        }
        return cleanedInts;
    }
}
