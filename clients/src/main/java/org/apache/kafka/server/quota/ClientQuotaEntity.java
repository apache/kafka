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
package org.apache.kafka.server.quota;

import java.util.List;

/**
 * The metadata for an entity for which quota is configured. Quotas may be defined at
 * different levels and `configEntities` gives the list of config entities that define
 * the level of this quota entity.
 */
public interface ClientQuotaEntity {

    /**
     * Entity type of a {@link ConfigEntity}
     */
    public enum ConfigEntityType {
        USER,
        CLIENT_ID,
        DEFAULT_USER,
        DEFAULT_CLIENT_ID
    }

    /**
     * Interface representing a quota configuration entity. Quota may be
     * configured at levels that include one or more configuration entities.
     * For example, {user, client-id} quota is represented using two
     * instances of ConfigEntity with entity types USER and CLIENT_ID.
     */
    public interface ConfigEntity {
        /**
         * Returns the name of this entity. For default quotas, an empty string is returned.
         */
        String name();

        /**
         * Returns the type of this entity.
         */
        ConfigEntityType entityType();
    }

    /**
     * Returns the list of configuration entities that this quota entity is comprised of.
     * For {user} or {clientId} quota, this is a single entity and for {user, clientId}
     * quota, this is a list of two entities.
     */
    List<ConfigEntity> configEntities();
}
