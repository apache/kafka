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
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.server.util.Json;

import java.io.*;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;


public class MetadataSchemaChecker {
    static int latestTag = -1;
    static int latestVersion = -1;
    public static void main(String[] args) {

        try {
            final String dir = System.getProperty("user.dir");
            String path = dir + "/metadata/src/main/resources/common/metadata/BrokerRegistrationChangeRecord.json";
            BufferedReader reader = new BufferedReader(new FileReader(path));
            for (int i = 0; i < 15; i++) {
                reader.readLine();
            }
            String content = "";
            for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                content += line;
            }
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonNode1 = objectMapper.readTree(content);
            JsonNode jsonNode2 = objectMapper.readTree(content);

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

            if (!fieldsNew.hasNext()) {
                throw new RuntimeException("New schema is missing field " + fieldOrig.getKey());
            }

            Map.Entry<String, JsonNode> fieldNew = fieldsNew.next();
            if (!Objects.equals(fieldOrig.getKey(), fieldNew.getKey())) {
                throw new RuntimeException("New schema has wrongly ordered field, " + fieldNew.getKey() + " should be " + fieldOrig.getKey());
            }
            if (Objects.equals(fieldOrig.getKey(), "apiKey")) {
                if (fieldOrig.getValue().asInt() != fieldNew.getValue().asInt()) {
                    throw new RuntimeException("Mismatching api keys");
                }
            }
            if (Objects.equals(fieldOrig.getKey(), "type")) {
                if (!Objects.equals(fieldOrig.getValue(), fieldNew.getValue())) {
                    throw new RuntimeException("Mismatching types");
                }
            }
            if (Objects.equals(fieldOrig.getKey(), "versions")) {
                if (!Objects.equals(fieldOrig.getValue(), fieldNew.getValue())) {
                    throw new RuntimeException("Mismatching versions");
                }
            }

            if (fieldOrig.getValue().isArray() && fieldNew.getValue().isArray()) {
                Iterator<JsonNode> iterOrig = fieldOrig.getValue().iterator();
                Iterator<JsonNode> iterNew = fieldNew.getValue().iterator();
                JsonNode nOrig, nNew;
                while (iterOrig.hasNext()) {
                    nOrig = iterOrig.next();
                    nNew = iterNew.next();
                    latestVersion = Character.getNumericValue(nOrig.get("versions").asText().charAt(0));

                    // check tagged field stuff
                    if (nOrig.has("tag")) {
                        if (!nNew.has("tag")) {
                            throw new RuntimeException("new schema is missing tagged field ");
                        }
                        if (!nNew.has("taggedVersions")) {
                            throw new RuntimeException("new schema tagged field is missing tagged version");
                        }


                        if (latestTag + 1 != Character.getNumericValue(nNew.get("tag").asText().charAt(0))) {
                            throw new RuntimeException(" tag from new schema not in numeric order");
                        }

                        latestTag = Character.getNumericValue(nOrig.get("tag").asText().charAt(0));
                    }
                    parser(nOrig, nNew);
                }

                // use this loop if there are new fields
                while(iterNew.hasNext()) {
                    parseNewField(iterNew.next());
                }

            } else if (fieldOrig.getValue().isContainerNode() && fieldOrig.getValue().isContainerNode()) {
                parser(fieldOrig.getValue(), fieldNew.getValue());
            }
        }
    }

    private static void parseNewField(JsonNode node) {
        if (!node.has("type")) {
            throw new RuntimeException("new field requires a type");
        }
        if (Character.getNumericValue(node.get("versions").asText().charAt(0)) != (latestVersion + 1)) {
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
