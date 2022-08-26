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

package org.apache.kafka.common.quota;

import java.util.Map;
import java.util.Objects;

/**
 * Describes a client quota entity, which is a mapping of entity types to their names.
 */
public class ClientQuotaEntity {

    private final Map<String, String> entries;

    /**
     * The type of an entity entry.
     */
    public static final String USER = "user";
    public static final String CLIENT_ID = "client-id";
    public static final String IP = "ip";

    public static boolean isValidEntityType(String entityType) {
        return Objects.equals(entityType, USER) ||
            Objects.equals(entityType, CLIENT_ID) ||
            Objects.equals(entityType, IP);
    }

    /**
     * Constructs a quota entity for the given types and names. If a name is null,
     * then it is mapped to the built-in default entity name.
     *
     * @param entries maps entity type to its name
     */
    public ClientQuotaEntity(Map<String, String> entries) {
        this.entries = entries;
    }

    /**
     * @return map of entity type to its name
     */
    public Map<String, String> entries() {
        return this.entries;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClientQuotaEntity that = (ClientQuotaEntity) o;
        return Objects.equals(entries, that.entries);
    }

    @Override
    public int hashCode() {
        return Objects.hash(entries);
    }

    @Override
    public String toString() {
        return "ClientQuotaEntity(entries=" + entries + ")";
    }
}
