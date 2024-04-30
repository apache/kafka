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

import kafka.zk.KafkaZkClient;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ConfigEntity {
    final Entity root;
    final Optional<Entity> child;

    public ConfigEntity(Entity root, Optional<Entity> child) {
        this.root = root;
        this.child = child;
    }

    public String fullSanitizedName() {
        return root.sanitizedName.orElse("") + child.map(s -> "/" + s.entityPath()).orElse("");
    }

    public List<ConfigEntity> getAllEntities(KafkaZkClient zkClient) {
        // Describe option examples:
        //   Describe entity with specified name:
        //     --entity-type topics --entity-name topic1 (topic1)
        //   Describe all entities of a type (topics/brokers/users/clients):
        //     --entity-type topics (all topics)
        //   Describe <user, client> quotas:
        //     --entity-type users --entity-name user1 --entity-type clients --entity-name client2 (<user1, client2>)
        //     --entity-type users --entity-name userA --entity-type clients (all clients of userA)
        //     --entity-type users --entity-type clients (all <user, client>s))
        //   Describe default quotas:
        //     --entity-type users --entity-default (Default user)
        //     --entity-type users --entity-default --entity-type clients --entity-default (Default <user, client>)
        if (!root.sanitizedName.isPresent()) {
            Stream<ConfigEntity> rootEntities = list(zkClient.getAllEntitiesWithConfig(root.entityType))
                    .stream()
                    .map(name -> new ConfigEntity(new Entity(root.entityType, Optional.of(name)), child));

            return child.map(s ->
                rootEntities.flatMap(rootEntity ->
                    new ConfigEntity(rootEntity.root, Optional.of(new Entity(s.entityType, Optional.empty()))).getAllEntities(zkClient).stream()))
                .orElse(rootEntities)
                .collect(Collectors.toList());
        } else {
            if (!child.isPresent())
                return Collections.singletonList(this);

            Entity childEntity = child.get();

            if (childEntity.sanitizedName.isPresent())
                return Collections.singletonList(this);
            else {
                return list(zkClient.getAllEntitiesWithConfig(root.entityPath() + "/" + childEntity.entityType))
                    .stream()
                    .map(name -> new ConfigEntity(root, Optional.of(new Entity(childEntity.entityType, Optional.of(name)))))
                    .collect(Collectors.toList());
            }
        }
    }

    @Override
    public String toString() {
        return root.toString() + child.map(s -> ", " + s).orElse("");
    }

    public static <V> List<V> list(Seq<V> seq) {
        List<V> res = new ArrayList<>();
        seq.foreach(v -> {
            res.add(v);
            return null;
        });
        return res;
    }
}
