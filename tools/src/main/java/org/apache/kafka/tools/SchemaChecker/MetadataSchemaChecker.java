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
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectLoader;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevTree;
import org.eclipse.jgit.revwalk.RevWalk;
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
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

public class MetadataSchemaChecker {

    static int latestTag = -1;
    static int oldLatestVersion = -1;
    static int oldFirstVersion = -1;
    static int newLatestVersion = -1;
    static int newFirstVersion = -1;
    public static void main(String[] args) throws Exception {
        /*ArgumentParser par = ArgumentParsers.newArgumentParser("metadata-schema-checker").
                defaultHelp(true).
                description("Metadata Schema Checker");
        par.addArgument("--schema", "-s").
                required(true).
                help("Changed schema");
        // need to split all of this up into separate  helper functions
        Namespace input = par.parseArgsOrFail(args);*/

        try {
            final String dir = System.getProperty("user.dir");
            //String path = dir + "/metadata/src/main/resources/common/metadata/" + input.get("schema").toString() + ".json";
            String path = dir + "/metadata/src/main/resources/common/metadata/BrokerRegistrationChangeRecord.json";
            BufferedReader reader = new BufferedReader(new FileReader(path));
            for (int i = 0; i < 15; i++) {
                reader.readLine();
            }
            StringBuilder content = new StringBuilder();
            for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                content.append(line);
            }


            //String path1 = dir + "/metadata/src/main/resources/common/metadata/AccessControlEntryRecord.json";
            String path1 = dir + "/tools/src/main/java/org/apache/kafka/tools/SchemaChecker/TestingSchema.json";
            BufferedReader reader1 = new BufferedReader(new FileReader(path1));
            for (int i = 0; i < 15; i++) {
                reader1.readLine();
            }
            StringBuilder content1 = new StringBuilder();
            for (String line1 = reader1.readLine(); line1 != null; line1 = reader1.readLine()) {
                content1.append(line1);
            }

            String gitContent = GetDataFromGit();


            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonNode1 = objectMapper.readTree(content.toString());
            JsonNode jsonNode2 = objectMapper.readTree(content1.toString());

            checkApiTypeVersions(jsonNode1, jsonNode2);
            parser((ArrayNode) jsonNode1.get("fields"), (ArrayNode) jsonNode2.get("fields"));

        } catch (FileNotFoundException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static String GetDataFromGit() throws IOException, GitAPIException {
        StringBuilder stringBuilder = new StringBuilder();
        Path tempDir = Files.createTempDirectory("tempDir");
        File tempFile = new File(tempDir.toString(), "");
        if (tempFile.createNewFile()) {
            System.out.println("temp file created: " + tempFile);
        }
        Git git = Git.cloneRepository()
                .setURI("https://github.com/apache/kafka.git")
                .setCredentialsProvider(new UsernamePasswordCredentialsProvider("mannoopj", "Olivewinner1!"))
                .setDirectory(tempFile)
                .setBranchesToClone(Collections.singleton("refs/heads/trunk"))
                .call();
        Repository repository = git.getRepository();
        ObjectId lastCommitId = repository.resolve(Constants.HEAD);
        try (RevWalk revWalk = new RevWalk(repository)) {
            RevCommit commit = revWalk.parseCommit(lastCommitId);
            RevTree tree = commit.getTree();
            System.out.println("Having tree: " + tree);
            try (TreeWalk treeWalk = new TreeWalk(repository)) {
                treeWalk.addTree(tree);
                treeWalk.setRecursive(true);
                treeWalk.setFilter(PathFilter.create("metadata/src/main/resources/common/metadata/BrokerRegistrationChangeRecord.json"));
                if (!treeWalk.next()) {
                    throw new IllegalStateException("Did not find expected file /metadata/src/main/resources/common/metadata/BrokerRegistrationChangeRecord.json");
                }
                ObjectId objectId = treeWalk.getObjectId(0);
                ObjectLoader loader = repository.open(objectId);

                // and then one can the loader to read the file
                String content = new String(loader.getBytes());
                String[] lines = content.split("\n");
                for(int i = 15; i < lines.length; i++) {
                    stringBuilder.append(lines[i]);
                }
            }
            revWalk.dispose();
        }
        tempDir.toFile().deleteOnExit();
        return stringBuilder.toString();
    }

    private static void checkApiTypeVersions(JsonNode original, JsonNode edited) {
        if (!Objects.equals(original.get("apiKey"), edited.get("apiKey"))) {
            throw new RuntimeException("New schema has wrong api key, " + edited.get("apiKey") + " should be " + original.get("apiKey"));
        }
        if (!Objects.equals(original.get("type"), edited.get("type"))) {
            throw new RuntimeException("New schema has wrong record type, " + edited.get("type") + " should be " + original.get("type"));
        }
        String oldValidVersions = String.valueOf(original.get("validVersions"));
        String newValidVersions = String.valueOf(edited.get("validVersions"));
        oldLatestVersion = Character.getNumericValue(oldValidVersions.charAt(oldValidVersions.length() - 2));
        newLatestVersion = Character.getNumericValue(newValidVersions.charAt(newValidVersions.length() - 2));
        if (oldLatestVersion != newLatestVersion && oldLatestVersion + 1 != newLatestVersion) {
            throw new RuntimeException("Invalid latest versions, can at most be one higher than the previous");
        }
        oldFirstVersion = Character.getNumericValue(oldValidVersions.charAt(1));
        newFirstVersion = Character.getNumericValue(newValidVersions.charAt(1));
        if (oldFirstVersion != newFirstVersion) {
            throw new RuntimeException("cannot change lower end of valid versions");
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
                throw new RuntimeException("New schema is missing field ");
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
                throw new RuntimeException("Fields must have same type. Expected: " + nodeOrig.get("type") + " Received: " + nodeNew.get("type"));
            }

            if (nodeOrig.isArray() && nodeNew.isArray()) {
                parser((ArrayNode) nodeOrig, (ArrayNode) nodeNew);
            }
        }

        while (iterNewNode.hasNext()) {
            parseNewField(iterNewNode.next());
        }
    }

    private static boolean parseVersion(JsonNode nodeOrig, JsonNode nodeNew) {
        String oldVersion = String.valueOf(nodeOrig.get("versions"));
        oldVersion = oldVersion.substring(1, oldVersion.length() - 1);
        String newVersion = String.valueOf(nodeNew.get("versions"));
        newVersion = newVersion.substring(1, newVersion.length() - 1);

        if (!Objects.equals(oldVersion, newVersion)) {
            if (oldVersion.contains("-") && !newVersion.contains("-")) {
                int cutoff = Character.getNumericValue(oldVersion.charAt(oldVersion.length() - 1));
                if (cutoff > Character.getNumericValue(newVersion.charAt(0))) {
                    parseNewField(nodeNew);
                    return true;
                } else {
                    throw new RuntimeException("new schema cannot reopen already closed field");
                }
            } else if (oldVersion.contains("-") && newVersion.contains("-")) {
                throw new RuntimeException("cannot changed already closed field");
            } else if (!oldVersion.contains("-") && newVersion.contains("-")) {
                if (!Objects.equals(oldVersion.charAt(0), newVersion.charAt(0))) {
                    throw new RuntimeException("cannot change lower end of ");
                }
                int cutoffVersion = Character.getNumericValue(newVersion.charAt(newVersion.length() - 1));
                if (cutoffVersion != newLatestVersion && cutoffVersion + 1 != newLatestVersion) {
                    throw new RuntimeException("Invalid closing version for field");
                }
            } else if (!oldVersion.contains("-") && !newVersion.contains("-")) {
                int oldInt = Character.getNumericValue(oldVersion.charAt(0));
                int newInt = Character.getNumericValue(newVersion.charAt(0));
                if (oldInt < newInt) {
                    parseNewField(nodeNew);
                    return true;
                } else {
                    throw new RuntimeException("new field needs to be on a new version");
                }
            }
        }
        return false;
    }

    private static void oldParser(JsonNode nodeOrig, JsonNode nodeNew) {
        Iterator<Map.Entry<String, JsonNode>> fieldsOrig = nodeOrig.fields();
        Iterator<Map.Entry<String, JsonNode>> fieldsNew = nodeNew.fields();
        while (fieldsOrig.hasNext()) {
            Map.Entry<String, JsonNode> fieldOrig = fieldsOrig.next();

            //there should be at least the same amount of new fields as there are old
            if (!fieldsNew.hasNext()) {
                throw new RuntimeException("New schema is missing field ");
            }

            //checks if items in json fields are same order, not invariant I think, could be removed
            Map.Entry<String, JsonNode> fieldNew = fieldsNew.next();
            if (!Objects.equals(fieldOrig.getKey(), fieldNew.getKey())) {
                throw new RuntimeException("New schema has wrongly ordered field, " + fieldNew.getKey() + " should be " + fieldOrig.getKey());
            }

            if (fieldOrig.getValue().isArray() && fieldNew.getValue().isArray()) {
                Iterator<JsonNode> iterOrig = fieldOrig.getValue().iterator();
                Iterator<JsonNode> iterNew = fieldNew.getValue().iterator();
                JsonNode nOrig = null, nNew;
                boolean isNewField = false;
                while (iterOrig.hasNext() || isNewField) {
                    if (!isNewField) {
                        nOrig = iterOrig.next();
                    }
                    if (!iterNew.hasNext()) {
                        throw new RuntimeException("New schema is missing field ");
                    }
                    nNew = iterNew.next();

                    isNewField = false;
                //checkIfNewFields(nOrig, nNew);

                    if (isNewField) {
                        continue;
                    }

                    checkTaggedFieldIfTag(nOrig, nNew);
                    //parser(nOrig, nNew);
                }

                // use this loop if there are new fields
                while (iterNew.hasNext()) {
                    parseNewField(iterNew.next());
                }

            } else if (fieldOrig.getValue().isArray() || fieldNew.getValue().isArray()) {
                throw new RuntimeException("array missing");
            }
        }
    }


    private static boolean checkTaggedFieldIfTag(JsonNode origNode, JsonNode newNode) {
        if (origNode.has("tag")) {
            if (!newNode.has("tag")) {
                //throw new RuntimeException("new schema is missing tagged field ");
                parseNewField(newNode);
                return true;
            }
            if (!newNode.has("taggedVersions")) {
                throw new RuntimeException("new schema tagged field is missing tagged version");
            }


            if (latestTag + 1 != Character.getNumericValue(newNode.get("tag").asText().charAt(0))) {
                throw new RuntimeException(" tag from new schema not in numeric order, " + Character.getNumericValue(newNode.get("tag").asText().charAt(0))
                                           + " should be "  + (latestTag + 1));
            }
            latestTag = Character.getNumericValue(origNode.get("tag").asText().charAt(0));
        }
        return false;
    }

    private static void parseNewField(JsonNode node) {
        if (oldLatestVersion + 1 != newLatestVersion) {
            throw new RuntimeException("New schemas with new fields need to be on the next version iteration");
        }
        if (newLatestVersion != Character.getNumericValue(String.valueOf(node.get("versions")).charAt(1))) {
            throw new RuntimeException("new field version not correct");
        }
        if (!node.has("type") && !node.has("fields")) {
            throw new RuntimeException("new field requires a type if it doesn't have fields node ");
        }
        if (Character.getNumericValue(node.get("versions").asText().charAt(0)) != (oldLatestVersion + 1)) {
            System.out.println(node);
            throw new RuntimeException("New field must be on next version");
        }
        if (node.has("tag")) {
            if (!node.has("taggedVersions")) {
                throw new RuntimeException("new tagged field is missing tagged versions");
            }
            if (Character.getNumericValue(node.get("tag").asText().charAt(0)) != latestTag + 1) {
                throw new RuntimeException("new tagged field is not on next sequential tag");
            }
        }


    }

}
