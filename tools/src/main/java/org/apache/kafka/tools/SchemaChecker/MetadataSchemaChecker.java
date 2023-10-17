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
import org.eclipse.jgit.api.CheckoutCommand;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.InitCommand;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.internal.storage.file.FileRepository;
import org.eclipse.jgit.lib.*;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevTree;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.storage.file.FileRepositoryBuilder;
import org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider;
import org.eclipse.jgit.treewalk.TreeWalk;
import org.eclipse.jgit.treewalk.filter.PathFilter;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class MetadataSchemaChecker {

    static int latestTag = -1;
    static int  latestTagVersion = -1;
    static int oldLatestVersion = -1;
    static int oldFirstVersion = -1;
    static int newLatestVersion = -1;
    static int newFirstVersion = -1;

    static String[] filesCheckMetadata = {"AccessControlEntryRecord.json", "BrokerRegistrationChangeRecord.json", "ClientQuotaRecord.json",
            "ConfigRecord.json", "DelegationTokenRecord.json", "FeatureLevelRecord.json", "FenceBrokerRecord.json", "NoOpRecord.json",
            "PartitionChangeRecord.json", "PartitionRecord.json", "ProducerIdsRecord.json", "RegisterBrokerRecord.json",
            "RemoveAccessControlEntryRecord.json", "RemoveTopicRecord.json", "RemoveUserScramCredentialRecord.json", "TopicRecord.json",
            "UnfenceBrokerRecord.json", "UnregisterBrokerRecord.json", "UserScramCredentialRecord.json", "ZkMigrationRecord.json"};
    public static void main(String[] args) throws Exception {

        try {
            List<String> localContent = new ArrayList<>();
            for(String jsonSchema: filesCheckMetadata) {
                final String dir = System.getProperty("user.dir");
                String path = dir + "/metadata/src/main/resources/common/metadata/" + jsonSchema;
                BufferedReader reader = new BufferedReader(new FileReader(path));
                for (int i = 0; i < 15; i++) {
                    reader.readLine();
                }
                StringBuilder content = new StringBuilder();
                for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                    content.append(line);
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

        } catch (FileNotFoundException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static List<String> GetDataFromGit() throws IOException, GitAPIException {
        List<String> gitSchemas = new ArrayList<>();

        Git git = Git.open(new File(System.getProperty("user.dir") + "/.git"));
        Repository repository = git.getRepository();
        Ref head = git.getRepository().getRefDatabase().firstExactRef("refs/heads/trunk");

        try (RevWalk revWalk = new RevWalk(repository)) {
            RevCommit commit = revWalk.parseCommit(head.getObjectId());
            RevTree tree = commit.getTree();
            for (String jsonSchema : filesCheckMetadata) {
                StringBuilder stringBuilder = new StringBuilder();
                try (TreeWalk treeWalk = new TreeWalk(repository)) {
                    treeWalk.addTree(tree);
                    treeWalk.setRecursive(true);
                    treeWalk.setFilter(PathFilter.create("metadata/src/main/resources/common/metadata/" + jsonSchema));
                    if (!treeWalk.next()) {
                        throw new IllegalStateException("Did not find expected file /metadata/src/main/resources/common/metadata/" + jsonSchema);
                    }
                    ObjectId objectId = treeWalk.getObjectId(0);
                    ObjectLoader loader = repository.open(objectId);

                    String content = new String(loader.getBytes());
                    String[] lines = content.split("\n");
                    boolean print = false;
                    for (int i = 15; i < lines.length; i++) {
                        if (lines[i].charAt(0) == '{') {
                            print = true;
                        }
                        if (print && !lines[i].contains("//")) {
                            stringBuilder.append(lines[i]);
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
            throw new IllegalStateException("New schema has wrong api key, " + edited.get("apiKey") + " should be " + original.get("apiKey"));
        }
        if (!Objects.equals(original.get("type"), edited.get("type"))) {
            throw new IllegalStateException("New schema has wrong record type, " + edited.get("type") + " should be " + original.get("type"));
        }
        String oldValidVersions = String.valueOf(original.get("validVersions"));
        String newValidVersions = String.valueOf(edited.get("validVersions"));
        oldLatestVersion = Character.getNumericValue(oldValidVersions.charAt(oldValidVersions.length() - 2));
        newLatestVersion = Character.getNumericValue(newValidVersions.charAt(newValidVersions.length() - 2));
        if (oldLatestVersion != newLatestVersion && oldLatestVersion + 1 != newLatestVersion) {
            throw new IllegalStateException("Invalid latest versions, can at most be one higher than the previous");
        }
        oldFirstVersion = Character.getNumericValue(oldValidVersions.charAt(1));
        newFirstVersion = Character.getNumericValue(newValidVersions.charAt(1));
        if (oldFirstVersion != newFirstVersion) {
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

        if (!Arrays.equals(oldVersion, newVersion)) {
            if (oldVersion.length == 2 && newVersion.length == 1) {
                int cutoff = Integer.parseInt(oldVersion[1]);
                if (cutoff > Integer.parseInt(newVersion[0])) {
                    parseNewField(nodeNew);
                    return true;
                } else {
                    throw new IllegalStateException("new schema cannot reopen already closed field");
                }
            } else if (oldVersion.length == 2 && newVersion.length == 2) {
                throw new IllegalStateException("cannot changed already closed field");
            } else if (oldVersion.length == 1 && newVersion.length == 2) {
                if (!Objects.equals(oldVersion[0], newVersion[0])) {
                    throw new IllegalStateException("cannot change lower end of ");
                }
                int cutoffVersion = Integer.parseInt(newVersion[1]);
                if (cutoffVersion != newLatestVersion && cutoffVersion + 1 != newLatestVersion) {
                    throw new IllegalStateException("Invalid closing version for field");
                }
            } else if (oldVersion.length == 1 && newVersion.length == 1) {
                int oldInt = Integer.parseInt(oldVersion[0]);
                int newInt = Integer.parseInt(newVersion[0]);
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
        if (nodeOrig.has("nullableVersions")) {
            if (!nodeNew.has("nullableVersions")) {
                throw new IllegalStateException("field is missing nullable information");
            }
            if (oldVersion.length == 2 && newVersion.length == 1) {
                throw new IllegalStateException("cannot make field nullable after closing nullable versions");
            }
            if (oldVersion.length == 1 && newVersion.length == 2) {
                if (!Objects.equals(oldVersion[0], newVersion[0])) {
                    throw new IllegalStateException("invalid closing version for nullable versions");
                }
                if (Integer.parseInt(newVersion[1]) != newLatestVersion) {
                    throw new IllegalStateException("incorrect closing version for nullable versions");
                }
            }

        } else if (nodeNew.has("nullableVersions")) {
            if (Integer.parseInt(newVersion[0]) != newLatestVersion) {
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
        if (oldLatestVersion + 1 != newLatestVersion && !node.has("tag")) {
            throw new IllegalStateException("New schemas with new fields need to be on the next version iteration");
        }
        if (newLatestVersion != Integer.parseInt(versions[0]) && !node.has("tag")) {
            throw new IllegalStateException("new field version not correct");
        }
        if (!node.has("type") && !node.has("fields")) {
            throw new IllegalStateException("new field requires a type if it doesn't have fields node ");
        }

        if (Integer.parseInt(versions[0]) != (oldLatestVersion + 1) && !node.has("tag")) {
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
            if (Integer.parseInt(versions[0]) < oldLatestVersion ||
                Integer.parseInt(versions[0]) > newLatestVersion) {
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
