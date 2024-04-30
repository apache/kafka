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
package kafka.admin;

import org.apache.kafka.common.utils.Sanitizer;
import org.apache.kafka.server.config.ConfigType;
import org.apache.kafka.server.config.ZooKeeperInternals;

import java.util.Optional;

public class Entity {
    final String entityType;
    final Optional<String> sanitizedName;

    public Entity(String entityType, Optional<String> sanitizedName) {
        this.entityType = entityType;
        this.sanitizedName = sanitizedName;
    }

    String entityPath() {
        return sanitizedName.map(n -> entityType + "/" + n).orElse(entityType);
    }

    @Override
    public String toString() {
        String typeName;
        switch (entityType) {
            case ConfigType.USER:
                typeName = "user-principal";
                break;
            case ConfigType.CLIENT:
                typeName = "client-id";
                break;
            case ConfigType.TOPIC:
                typeName = "topic";
                break;
            default:
                typeName = entityType;
        }

        return sanitizedName.map(n -> {
            if (n.equals(ZooKeeperInternals.DEFAULT_STRING))
                return "default " + typeName;

            String desanitized = (entityType.equals(ConfigType.USER) || entityType.equals(ConfigType.CLIENT)) ? Sanitizer.desanitize(n) : n;
            return typeName + " '" + desanitized + "'";
        }).orElse(entityType);
    }
}
