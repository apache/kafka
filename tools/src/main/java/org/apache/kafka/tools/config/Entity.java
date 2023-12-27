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

package org.apache.kafka.tools.config;

import org.apache.kafka.common.utils.Sanitizer;
import org.apache.kafka.server.config.ConfigEntityName;
import org.apache.kafka.server.config.ConfigType;

import java.util.Objects;
import java.util.Optional;

public class Entity {
    final String entityType;
    final Optional<String> sanitizedName;

    public Entity(String entityType, Optional<String> sanitizedName) {
        this.entityType = entityType;
        this.sanitizedName = sanitizedName;
    }

    public String entityPath() {
        return sanitizedName.map(n -> entityType + "/" + n).orElse(entityType);
    }

    @Override
    public String toString() {
        String typeName;
        if (Objects.equals(ConfigType.USER, entityType))
            typeName = "user-principal";
        else if (Objects.equals(ConfigType.CLIENT, entityType))
            typeName = "client-id";
        else if (Objects.equals(ConfigType.TOPIC, entityType))
            typeName =  "topic";
        else
            typeName = entityType;

        return sanitizedName.map(n -> {
            if (Objects.equals(n, ConfigEntityName.DEFAULT))
                return "default " + typeName;
            String desanitized = (Objects.equals(entityType, ConfigType.USER) || Objects.equals(entityType, ConfigType.CLIENT))
                ? Sanitizer.desanitize(n)
                : n;

            return typeName + " '" + desanitized + "'";
        }).orElse(entityType);
    }
}
