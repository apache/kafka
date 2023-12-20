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

package org.apache.kafka.image.node;

import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.image.ClientQuotaImage;
import org.apache.kafka.image.ClientQuotasImage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.common.quota.ClientQuotaEntity.CLIENT_ID;
import static org.apache.kafka.common.quota.ClientQuotaEntity.IP;
import static org.apache.kafka.common.quota.ClientQuotaEntity.USER;


public class ClientQuotasImageNode implements MetadataNode {
    /**
     * The name of this node.
     */
    public static final String NAME = "clientQuotas";

    /**
     * The topics image.
     */
    private final ClientQuotasImage image;

    public ClientQuotasImageNode(ClientQuotasImage image) {
        this.image = image;
    }

    @Override
    public Collection<String> childNames() {
        ArrayList<String> childNames = new ArrayList<>();
        for (ClientQuotaEntity entity : image.entities().keySet()) {
            childNames.add(clientQuotaEntityToString(entity));
        }
        return childNames;
    }

    static String clientQuotaEntityToString(ClientQuotaEntity entity) {
        if (entity.entries().isEmpty()) {
            throw new RuntimeException("Invalid empty entity");
        }
        String clientId = null;
        String ip = null;
        String user = null;
        for (Map.Entry<String, String> entry : entity.entries().entrySet()) {
            if (entry.getKey().equals(CLIENT_ID)) {
                clientId = entry.getValue();
            } else if (entry.getKey().equals(IP)) {
                ip = entry.getValue();
            } else if (entry.getKey().equals(USER)) {
                user = entry.getValue();
            } else {
                throw new RuntimeException("Invalid entity type " + entry.getKey());
            }
        }
        StringBuilder bld = new StringBuilder();
        String prefix = "";
        if (clientId != null) {
            bld.append(prefix).append("clientId(").append(escape(clientId)).append(")");
            prefix = "_";
        }
        if (ip != null) {
            bld.append(prefix).append("ip(").append(escape(ip)).append(")");
            prefix = "_";
        }
        if (user != null) {
            bld.append(prefix).append("user(").append(escape(user)).append(")");
            prefix = "_";
        }
        return bld.toString();
    }

    static String escape(String input) {
        return input.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)");
    }

    @Override
    public MetadataNode child(String name) {
        ClientQuotaEntity entity = decodeEntity(name);
        if (entity == null) return null;
        ClientQuotaImage clientQuotaImage = image.entities().get(entity);
        if (clientQuotaImage == null) return null;
        return new ClientQuotaImageNode(clientQuotaImage);
    }

    static ClientQuotaEntity decodeEntity(String input) {
        Map<String, String> entries = new HashMap<>();
        String type = null;
        String value = "";
        boolean escaping = false;
        int i = 0;
        while (true) {
            if (i >= input.length()) return null;
            if (type == null) {
                if (input.substring(i).startsWith("clientId(")) {
                    type = CLIENT_ID;
                    i += "clientId(".length();
                } else if (input.substring(i).startsWith("ip(")) {
                    type = IP;
                    i += "ip(".length();
                } else if (input.substring(i).startsWith("user(")) {
                    type = USER;
                    i += "user(".length();
                } else {
                    return null;
                }
            } else {
                char c = input.charAt(i++);
                if (escaping) {
                    value += c;
                    escaping = false;
                } else {
                    switch (c) {
                        case ')':
                            entries.put(type, value);
                            type = null;
                            value = "";
                            break;
                        case '\\':
                            escaping = true;
                            break;
                        default:
                            value += c;
                            break;
                    }
                }
                if (type == null) {
                    if (i >= input.length()) {
                        return new ClientQuotaEntity(entries);
                    } else if (input.charAt(i++) != '_') {
                        return null;
                    }
                }
            }
        }
    }
}
