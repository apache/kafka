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

package org.apache.kafka;



import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;


import java.io.*;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

public class MetadataSchemaChecker {

    static int latestTag = -1;
    static int latestVersion = -1;
    public static void main(String[] args) throws Exception{
        ArgumentParser
        // need to split all of this up into separate  helper functions

        try {
            final String dir = System.getProperty("user.dir");
            String path = dir + "/metadata/src/main/resources/common/metadata/RegisterBrokerRecord.json";
            BufferedReader reader = new BufferedReader(new FileReader(path));
            for (int i = 0; i < 15; i++) {
                reader.readLine();
            }
            StringBuilder content = new StringBuilder();
            for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                content.append(line);
            }


            String path1 = dir + "/metadata/src/test/java/org/apache/kafka/TestingSchema.json";
            BufferedReader reader1 = new BufferedReader(new FileReader(path1));
            for (int i = 0; i < 15; i++) {
                reader1.readLine();
            }
            StringBuilder content1 = new StringBuilder();
            for (String line1 = reader1.readLine(); line1 != null; line1 = reader1.readLine()) {
                content1.append(line1);
            }

            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonNode1 = objectMapper.readTree(content.toString());
            JsonNode jsonNode2 = objectMapper.readTree(content1.toString());



            if (!Objects.equals(jsonNode1.get("apiKey"), jsonNode2.get("apiKey"))) {
                throw new RuntimeException("New schema has wrong api key, " + jsonNode2.get("apiKey") + " should be " + jsonNode1.get("apiKey"));
            }
            if (!Objects.equals(jsonNode1.get("type"), jsonNode2.get("type"))) {
                throw new RuntimeException("New schema has wrong record type, " + jsonNode2.get("type") + " should be " + jsonNode1.get("type"));
            }
            String validVersions = String.valueOf(jsonNode1.get("validVersions"));
            latestVersion = Character.getNumericValue(validVersions.charAt(validVersions.length() - 2));

            parser(jsonNode1, jsonNode2);



        } catch (FileNotFoundException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void parser(JsonNode nodeOrig, JsonNode nodeNew) {
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

                    isNewField = checkNewFields(nOrig, nNew);

                    if (isNewField) {
                        continue;
                    }

                    checkTaggedFieldIfTag(nOrig, nNew);
                    parser(nOrig, nNew);
                }

                // use this loop if there are new fields
                while(iterNew.hasNext()) {
                    parseNewField(iterNew.next());
                }

            } else if (fieldOrig.getValue().isArray() || fieldNew.getValue().isArray()) {
                throw new RuntimeException("array missing");
            }
        }
    }

    private static Boolean checkNewFields(JsonNode origNode, JsonNode newNode) {
        if (!Objects.equals(origNode.get("versions"), newNode.get("versions"))) {
            parseNewField(newNode);
            return true;
        }
        return false;
    }

    private static void checkTaggedFieldIfTag(JsonNode origNode, JsonNode newNode) {
        if (origNode.has("tag")) {
            if (!newNode.has("tag")) {
                throw new RuntimeException("new schema is missing tagged field ");
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
    }

    private static void parseNewField(JsonNode node) {
        //System.out.println(node);
        if (!node.has("type") && !node.has("fields")) {
            throw new RuntimeException("new field requires a type if it doesn't have fields node ");
        }
        if (Character.getNumericValue(node.get("versions").asText().charAt(0)) != (latestVersion + 1)) {
            System.out.println(node);
            throw new RuntimeException("New field must be on next version");
        }
        if (node.has("tag")) {
            if (!node.has("taggedVersions")) {
                throw new RuntimeException("new tagged field is missing tagged versions");
            }
            if (Character.getNumericValue(node.get("taggedVersions").asText().charAt(0)) != latestTag + 1) {
                throw new RuntimeException("new tagged field is not on next sequential tag");
            }
        }


    }

}
