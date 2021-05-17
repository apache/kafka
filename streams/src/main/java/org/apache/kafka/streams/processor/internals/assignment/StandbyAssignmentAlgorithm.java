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
//package org.apache.kafka.streams.processor.internals.assignment;
//
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.List;
//import java.util.Map;
//import java.util.Set;
//import java.util.UUID;
//import java.util.function.Function;
//
//public class StandbyAssignmentAlgorithm {
//    private final Map<String, Set<UUID>> tagToClientsMapping;
//    private final Map<String, Set<String>> allTags;
//    private final Map<String, Integer> capacityPerTag;
//
//    public StandbyAssignmentAlgorithm(final Map<String, Set<UUID>> tagToClientsMapping,
//                                      final Map<String, Set<String>> allTags,
//                                      final Function<String, String> tagValueToTagKeyProvider) {
//        this.tagToClientsMapping = tagToClientsMapping;
//        this.allTags = new HashMap<>(allTags);
//        this.tagValueToTagKeyProvider = tagValueToTagKeyProvider;
//    }
//
//    public Set<UUID> invalidatedClients(final Map<String, String> clientTags) {
//        final Set<UUID> result = new HashSet<>();
//
//        clientTags.forEach((tagKey, tagValue) -> {
//            final Set<UUID> clients = tagToClientsMapping.get(tagValue);
//
//            result.addAll(clients);
//        });
//
//        return result;
//    }
//
//    public void invalidateTags(final Map<String, String> clientTags) {
//        clientTags.forEach((tagKey, tagValue) -> {
//            final Set<String> tagValues = allTags.get(tagKey);
//            tagToClientsMapping.remove(tagValue);
//            tagValues.remove(tagValue);
//        });
//    }
//
//    public void resetWith(final Map<String, Set<UUID>> tagToClientsMapping) {
//        this.tagToClientsMapping.clear();
//        this.tagToClientsMapping.putAll(tagToClientsMapping);
//    }
//
//
//}
