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

package org.apache.kafka.trogdor.workload;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.kafka.trogdor.common.StringExpander;
import org.apache.kafka.trogdor.rest.Message;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * TopicsSpec maps topic names to descriptions of the partitions in them.
 *
 * In JSON form, this is serialized as a map whose keys are topic names,
 * and whose entries are partition descriptions.
 * Keys may also refer to multiple partitions.  For example, this specification
 * refers to 3 topics foo1, foo2, and foo3:
 *
 * {
 *   "foo[1-3]" : {
 *      "numPartitions": 3
 *      "replicationFactor": 3
 *    }
 * }
 */
public class TopicsSpec extends Message {
    public static final TopicsSpec EMPTY = new TopicsSpec().immutableCopy();

    private final Map<String, PartitionsSpec> map;

    @JsonCreator
    public TopicsSpec() {
        this.map = new HashMap<>();
    }

    private TopicsSpec(Map<String, PartitionsSpec> map) {
        this.map = map;
    }

    @JsonAnyGetter
    public Map<String, PartitionsSpec> get() {
        return map;
    }

    @JsonAnySetter
    public void set(String name, PartitionsSpec value) {
        map.put(name, value);
    }

    public TopicsSpec immutableCopy() {
        HashMap<String, PartitionsSpec> mapCopy = new HashMap<>();
        mapCopy.putAll(map);
        return new TopicsSpec(Collections.unmodifiableMap(mapCopy));
    }

    /**
     * Enumerate the partitions inside this TopicsSpec.
     *
     * @return      A map from topic names to PartitionsSpec objects.
     */
    public Map<String, PartitionsSpec> materialize() {
        HashMap<String, PartitionsSpec> all = new HashMap<>();
        for (Map.Entry<String, PartitionsSpec> entry : map.entrySet()) {
            for (String topicName : StringExpander.expand(entry.getKey())) {
                all.put(topicName, entry.getValue());
            }
        }
        return all;
    }
}
